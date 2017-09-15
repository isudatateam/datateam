<?php 
include_once "../include/myview.php";
$t = new MyView();
$t->title = "ISU Data Team!";
$t->content = <<<EOF

<h3>ISU Data Team Contacts</h3>

<p>Feel free to email us at <a href="mailto:isudatateam@iastate.edu">isudatateam@iastate.edu</a></p>

<p><img src="images/dt-160307.jpg" class="img img-responsive"></p>

EOF;
$t->render('single.phtml');
?>
