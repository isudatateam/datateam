var TABS = ['sites', 'treatments', 'agronomic', 'soil', 'year', 'option'];
var CURRENTTAB = 0;

function applyFilter(data){
	//console.log(data);
	$('#treatments-ui input').prop('disabled', true);
	$.each(data.treatments, function(idx, v){
		console.log("TREAT: " + v);
		$("#treatments-ui input[data-treatment='"+v+"']").prop('disabled', false);
	});
		
	$('#agronomic-ui input').prop('disabled', true);
	$.each(data.agronomic, function(idx, v){
		//console.log("AG: " + v);
		$("#agronomic-ui input[data-agronomic='"+v+"']").prop('disabled', false);
	});
		
	$('#soil-ui input').prop('disabled', true);
	$.each(data.soil, function(idx, v){
		//console.log("SOIL: " + v);
		$("#soil-ui input[data-soil='"+v+"']").prop('disabled', false);
	});

	$('#year-ui input').prop('disabled', true);
	$.each(data.year, function(idx, v){
		//console.log("YEAR: " + v);
		$("#year-ui input[data-year='"+v+"']").prop('disabled', false);
	});
		
	
}

function build_data(){
	var data = {sites: [], treatments: [], agronomic: [], soil: [],
			year: [], management: [], option: []};

	// Which sites are checked
	$('#sites-ui input:checked').each(function(idx, elem){
		data.sites.push($(elem).val());
	});
	// Which treatments are checked
	$('#treatments-ui input:checked').each(function(idx, elem){
		data.treatments.push($(elem).val());
	});
	// Which agronomic are checked
	$('#agronomic-ui input:checked').each(function(idx, elem){
		data.agronomic.push($(elem).val());
	});
	// Which soil are checked
	$('#soil-ui input:checked').each(function(idx, elem){
		data.soil.push($(elem).val());
	});
	return data;
}

function runfilter(){
	// Call the server to filter the display based on what we have
	// selected
	$.ajax({
		type: "POST",
		url: 'filter.py',
		data: build_data(),
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