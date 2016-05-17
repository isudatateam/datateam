var TABS = ['site', 'treatment', 'agronomic', 'soil', 'year', 'management'];
var CURRENTTAB = 0;


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

	$("#next_btn").click(function(){
		var currentdiv = TABS[CURRENTTAB] +"-ui";
		$("#"+currentdiv).css("display", "none");
		var currentdiv = TABS[CURRENTTAB+1] +"-ui";
		$("#"+currentdiv).css("display", "block");
		CURRENTTAB += 1;
		
	});
});