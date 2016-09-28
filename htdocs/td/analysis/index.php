<?php 
include_once "../../../include/myview.php";
$t = new MyView();
$t->thispage = "td-analysis";
$t->title = "TD Analysis";
$t->content = <<<EOF

<h3>Write your HTML content here</h3>
		
<a href="bah.html">Whatever link</a>

EOF;
$t->render('single.phtml');

?>
