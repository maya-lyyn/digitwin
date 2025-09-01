#Import All necessary libraries 
import json
from boto3 import resource
from boto3 import client
from sklearn.externals.joblib import load
import pandas as pd
import numpy as np
from datetime import timedelta
import aqi
import datetime
import warnings
warnings.filterwarnings("ignore")
#import os #Riaz
import traceback #Riaz
from subprocess import call #Riaz

def get_date(a):
    d_obj=datetime.datetime.strptime(a, "%m-%d-%Y %H")
    return d_obj
                   
#Daterange function (generates datetimes which include both start_date and end_date)
def daterange(start_date, end_date):
    delta = timedelta(hours=1)
    while start_date <= end_date:
        yield start_date
        start_date += delta    

#Bus emission function
def bus_emissions(number_buses, number_hours):
        PM2_5 = 0.31*(number_buses)*(number_hours)
        return aqi.to_iaqi(aqi.POLLUTANT_PM25, PM2_5, algo=aqi.ALGO_EPA)


def lambda_handler(event,context):
    try: 
        #S3 details
        access_key_id = "secret" #20 characters
        secret_access_key = "secret" #40 characters
        bucket_name = "digitwin-airqualitydata"
        region_name = 'us-east-1'
        subdir1 = "EPA_data" #epa subdirectory
        subdir2="purple_air_data" #purple air subdirectory
        
        call('rm -rf /tmp/*', shell=True) #Riaz
        #os.system('rm -rf /tmp/*') #Riaz
        
        #S3 resource object
        s3 = resource(
            's3',
            region_name=region_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        #S3 client object
        s3_1= client(
            's3',
            region_name=region_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        ##Get each feature from event dictionary
        

        #s_date=start datetime; e_date=end datetime; epa=epa boolean; ppa=Purple Air boolean; bus=bus simulation boolean; 
        #num_bus=number of buses; bus_start= bus start datetime; bus_end=bus end datetime
        s_date=event.get("date_start");e_date=event.get("date_end");bus=event.get("bus"); num_bus=event.get("number")
        bus_start=event.get("bus_start"); bus_end=event.get("bus_end") 
        
        
        #Model Names
        temp_name="EPA_Temp_regress.sav"
        humid_name="EPA_humid_regress.sav"
        aqi_name="EPA_rfr_temp_humid.sav"
        regression_k='joblib_regression.sav'
        temp_k='joblib_regression_temp.sav'
        hum_k='joblib_regression_humidity.sav'


        #Download EPA models from S3 to /tmp     
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir1, temp_name), "/tmp/{}".format(temp_name))
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir1, humid_name), "/tmp/{}".format(humid_name))
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir1, aqi_name), "/tmp/{}".format(aqi_name))
        
        #Download PPA models from S3 to /tmp 
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir2, regression_k), "/tmp/{}".format(regression_k))
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir2, temp_k), "/tmp/{}".format(temp_k))
        s3.Bucket(bucket_name).download_file("{}/{}".format(subdir2, hum_k), "/tmp/{}".format(hum_k))


        #Load EPA models
        regress_temp_e=load("/tmp/{}".format(temp_name))
        regress_hum_e=load("/tmp/{}".format(humid_name))
        regress_aqi_e=load("/tmp/{}".format(aqi_name))
        
        #Load PPA models
        regress_aqi_p=load("/tmp/{}".format(regression_k))
        regress_temp_p=load("/tmp/{}".format(temp_k))
        regress_hum_p=load("/tmp/{}".format(hum_k))

        
            
        #Defining Start_datetime and End_datetime
        start_date=get_date(s_date) 
        end_date=get_date(e_date)
        
        #Set end_date to start_date if  end_date-start_date is less than 1 hour
        if end_date-start_date <timedelta(hours=1):
            end_date=start_date
            
        P=[] #A list containing lists of[year,month,day,hour,minute,second] for each start_date +timedelta(hours=1)<=end_date 
        P_1=[] #same as list P but for ppa
        hours=pd.Series(pd.date_range(s_date, e_date, freq='1H'))
        B=list(hours.dt.strftime('%m-%d-%Y %H'))#list to contain dates

        for single_date in daterange(start_date, end_date):
         d = datetime.datetime.strptime(str(single_date), "%Y-%m-%d %H:%M:%S")
         P.append([d.month,d.day,d.year,d.hour,d.minute,d.second])
         P_1.append([d.month,d.day,d.year,d.hour,d.minute,d.second]);   
        
       
        #Get dataframe of actual AQI values PPA
        actual_aqi_name= "ac_aqi_purpleee.csv"
        obj = s3_1.get_object(Bucket= bucket_name, Key= "{}/{}".format(subdir2, actual_aqi_name)) 
        initial_df=pd.read_csv(obj['Body'])
        actual_aqi=hours.isin(initial_df['Datetime'])*1
        actual_aqi=actual_aqi.replace({0: None})
        actual_aqi=list(actual_aqi)
        shared_datetime=hours[hours.isin(initial_df['Datetime'])]
        shared_indexes=list(shared_datetime.index)
        initial_df=initial_df.drop_duplicates()
        initial_df=initial_df.set_index('Datetime')
        initial_df=initial_df.squeeze()
        for x in shared_indexes: 
            actual_aqi[x]=initial_df.loc[str(shared_datetime[x])]
          
           
            
        
        #Predicting Humidity for epa 
        humid_e=regress_hum_e.predict(P)
        
        #Predicting EPA Temp
        temp_e=regress_temp_e.predict(P)   
        #Dictionary for Days of the Week
        pl_values = {
                    6 : 1,
                    0 : 2,
                    1 : 3,
                    2 : 4,
                    3 : 5,
                    4 : 6,
                    5 : 7,
                }
        for x in range(len(P)):
                Date= datetime.datetime(P[x][2],P[x][0],P[x][1])
                pl=Date.weekday()
                #set 'e' equalto a number based on which day of the week that corresponds to user's input
                e = pl_values[pl]
                #Conditional for  whether date entered is a weekend or weekday
                if e!=7 or e!=1:f=False
                else:f=True
                P[x].append(e)
                P[x].append(f)
                P[x].append(temp_e[x])
                P[x].append(humid_e[x])
                del e,f
                
        #Predicting EPA AQI
        pred_aqi_e=regress_aqi_e.predict(P)
        
        #Predicting Humidity for ppa 
        humid_p=regress_hum_p.predict(P_1)
        
        #appending humidity into P_1 for PPA temp prediction
        for x in range(len(P_1)):P_1[x].append(humid_p[x]) 
            
        #predicting  temperature data from model
        temp_p=regress_temp_p.predict(P_1)
        
        #appending temp into P_1 for PPA AQI prediction
        for x in range(len(P_1)):P_1[x].append(temp_p[x])

        #Predicting PPA AQI
        pred_aqi_p=regress_aqi_p.predict(P_1)

        #Load data into data frame
        data={
          "date":B,
          "aqi_EPA":pred_aqi_e,
          "aqi_PPA":pred_aqi_p,  
          "Actual AQI_PPA":actual_aqi, 
              }  
        df=pd.DataFrame(data)   

        #For Bus simulation 
        #if bus==False:results_J=df[['date', 'aqi_EPA','aqi_PPA','Actual AQI_PPA','bus']].to_json(orient="records")
        #else:
        V = hours.dt.hour #series of hours from start_date to end_date in 1 hour intervals
        F=pd.Series(pd.date_range(bus_start, bus_end, freq="1H").strftime('%H')) #series of hours from bus_start to bus_end in 1 hour intervals 
        L=V.isin(F) # comparing series V and F. Outputs series with  True or False
        L=np.array(L*1) #Changing Series with True or False to 1's and 0's and changing series to array
        pred_aqi_bus_e = (float(bus_emissions(num_bus,1))*L) +  pred_aqi_e #EPA bus
        pred_aqi_bus_p = (float(bus_emissions(num_bus,1))*L) +  pred_aqi_p #PPA bus
        df['aqibus_EPA']=pred_aqi_bus_e;df['aqibus_PPA']=pred_aqi_bus_p;
        results_J=df[['date', 'aqi_EPA','aqi_PPA', 'aqibus_EPA','aqibus_PPA','Actual AQI_PPA']].to_json(orient="records")
        
        
    except Exception as e:
        traceback.print_exc() #Riaz
        error = str(e)

        results_J = {
            'error' : error
        }

    finally:
        #os.system('rm -rf /tmp/*') #Riaz
        call('rm -rf /tmp/*', shell=True) #Riaz
        return results_J
