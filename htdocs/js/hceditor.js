var myPoint = null;

function showEditForm(){
	$('#edit_info').css('display', 'none');
	$('#edit_div').css('display', 'block');	
}
function hideEditForm(){
	$('#edit_div').css('display', 'none');	
	$('#edit_info').css('display', 'block');
}

function updateServer(newval){
	var dt = new Date(myPoint.x);
	var data = {table: EDIT_TABLE,
	            valid: dt.toISOString(),
	            uniqueid: EDIT_UNIQUEID,
	            column: EDIT_COLUMN,
	            plotid: myPoint.series.name,
	            comment: $("#edit_comment").val(),
	            value: newval};
	$('#edit_waiting').css('display', 'block');
	$.post( "edit.py", data)
	  .done(function( data ) {
	    //console.log(data);
	    $('#edit_log').append("<li>Server responded with: "+ data.status +"</li>");
		$('#edit_waiting').css('display', 'none');
	  });
}


$(document).ready(function(){
	$("#edit_save").click(function(){
		myPoint.update(parseFloat($('#edit_value').val()));
		updateServer($('#edit_value').val());
	});
	$("#edit_delete").click(function(){
		hideEditForm();
		updateServer(null);
		myPoint.remove();
		myPoint = null;
	});
});

function editPoint(pt){
	// High Charts callback when we click on a point
	myPoint = pt;
	showEditForm();
	//console.log(pt);
	var dt = new Date(pt.x);
	$('#edit_plot').html(pt.series.name);
	$('#edit_time').html(dt.toISOString());
	$('#edit_value').val(pt.y);
	
}
