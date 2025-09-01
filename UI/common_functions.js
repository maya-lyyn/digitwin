function showSuccess(success_text){
	$("#header-success").text(success_text);
	$('#success-modal').modal({show:true});
	error_time = parseInt($('.round-time-bar').css('--duration')) * 1000
	setTimeout(function(){ $('#success-modal').modal('hide'); }, error_time);
}

function showError(error_text){
	$("#header-error").text(error_text);
	$('#error-modal').modal({show:true});
	error_time = parseInt($('.round-time-bar').css('--duration')) * 1000
	setTimeout(function(){ $('#error-modal').modal('hide'); }, error_time);
}
