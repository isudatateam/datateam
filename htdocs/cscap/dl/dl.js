var TABS = ['sites', 'treatments', 'agronomic', 'soil', 'year', 'management',
            'option'];
var CURRENTTAB = 0;

function applyFilter(data){
	// NOTE: We do not backfilter sites
	if (data.treatments){
		$('#treatments-ui input').prop('disabled', true);
		$.each(data.treatments, function(idx, v){
			console.log(v);
			$("#treatments-ui input[data-treatment='"+v+"']").prop('disabled', false);
		});
		
	}
	
}

function runfilter(){
	// Call the server to filter the display based on what we have
	// selected
	var data = {sites: [], treatments: []};
	// Which sites are checked
	$('#sites-ui input:checked').each(function(idx, elem){
		data.sites.push($(elem).val());
	});
	// Which treatments are checked
	$('#treatments-ui input:checked').each(function(idx, elem){
		data.treatments.push($(elem).val());
	});
	$.ajax({
		type: "POST",
		url: 'filter.py',
		data: data,
		success: applyFilter
	});
};


$(document).ready(function(){

	$(".site-check").click(function(){
		var state = $(this).attr('data-state');
		if (! this.checked){
			$(".state-check[value='"+state+"']").prop('checked', false);	
		}
	});

	$(".state-check").click(function(){
		var state = $(this).val();
		$("input[data-state='"+state+"']").prop('checked', this.checked);
	});

	$("#next-btn, #prev-btn").click(function(){
		runfilter();
		var increment = ($(this).attr('id') == 'prev-btn') ? -1 : 1;
		var currentdiv = TABS[CURRENTTAB] +"-ui";
		$("#"+currentdiv).css("display", "none");
		var btndiv = TABS[CURRENTTAB] +"-btn";
		$("#"+btndiv).removeClass().addClass("btn btn-default");
		
		CURRENTTAB += increment;

		var currentdiv = TABS[CURRENTTAB] +"-ui";
		$("#"+currentdiv).css("display", "block");
		var btndiv = TABS[CURRENTTAB] +"-btn";
		$("#"+btndiv).removeClass().addClass("btn btn-primary");

		$('#next-btn').prop('disabled', ((CURRENTTAB + 1) == TABS.length));
		$('#prev-btn').prop('disabled', (CURRENTTAB  == 0));
		
	});
});