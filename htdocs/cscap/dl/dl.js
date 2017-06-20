var TABS = ['sites', 'treatments', 'agronomic', 'soil', 'ghg', 'water', 'ipm',
	'year', 'option'];
var CURRENTTAB = 0;

function applyFilter(data){
	//console.log(data);
	$('#treatments-ui input').prop('disabled', true);
	$.each(data.treatments, function(idx, v){
		//console.log("TREAT: " + v);
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

	$('#ghg-ui input').prop('disabled', true);
	$.each(data.ghg, function(idx, v){
		//console.log("GHG: " + v);
		$("#ghg-ui input[data-ghg='"+v+"']").prop('disabled', false);
	});

	$('#water-ui input').prop('disabled', true);
	$.each(data.water, function(idx, v){
		//console.log("SOIL: " + v);
		$("#water-ui input[data-water='"+v+"']").prop('disabled', false);
	});

	$('#ipm-ui input').prop('disabled', true);
	$.each(data.ipm, function(idx, v){
		//console.log("SOIL: " + v);
		$("#ipm-ui input[data-ipm='"+v+"']").prop('disabled', false);
	});

	
	$('#year-ui input').prop('disabled', true);
	$.each(data.year, function(idx, v){
		//console.log("YEAR: " + v);
		$("#year-ui input[data-year='"+v+"']").prop('disabled', false);
	});
		
	
}

function build_data(){
	var data = {sites: [], treatments: [], agronomic: [], soil: [],
			ghg: [], water: [], ipm: [],
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
	$('#ghg-ui input:checked').each(function(idx, elem){
		data.ghg.push($(elem).val());
	});
	$('#water-ui input:checked').each(function(idx, elem){
		data.water.push($(elem).val());
	});
	$('#ipm-ui input:checked').each(function(idx, elem){
		data.ipm.push($(elem).val());
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

function build_ui(){
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
	$(".fauxtabs button").click(function() {
		runfilter();
		var tabtitle = $(this).attr("id").replace("-btn", "");
		var currentdiv = TABS[CURRENTTAB] +"-ui";
		$("#"+currentdiv).css("display", "none");
		var btndiv = TABS[CURRENTTAB] +"-btn";
		$("#"+btndiv).removeClass().addClass("btn btn-default");
		CURRENTTAB = TABS.indexOf(tabtitle);

		var currentdiv = TABS[CURRENTTAB] +"-ui";
		$("#"+currentdiv).css("display", "block");
		var btndiv = TABS[CURRENTTAB] +"-btn";
		$("#"+btndiv).removeClass().addClass("btn btn-primary");

		$('#next-btn').prop('disabled', ((CURRENTTAB + 1) == TABS.length));
		$('#prev-btn').prop('disabled', (CURRENTTAB  == 0));
		
	});
	$(".sa").click(function(){
		var tabtitle = $(this).attr("id").replace("-selectall", "");
		$("#" + tabtitle +"-ui input:enabled[type='checkbox']").prop('checked', true);
	});
};

$(document).ready(function(){
	build_ui();
});