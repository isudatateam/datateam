<?php 
include_once "../include/myview.php";
$t = new MyView();
$t->title = "ISU Data Team!";
$t->content = <<<EOF

<h3>ISU Data Team Contacts</h3>

<p>Questions regarding the research database such as experimental designs, farm management operations, 
research and metadata collected, and protocols should be directed to 
<a href="mailto:isudatateam@iastate.edu">isudatateam@iastate.edu</a>. 
The team members listed below will answer your questions. We will determine if contacting the individual 
research site faculty and staff members are necessary.</p>

<p>Lori Abendroth
Sustainable Corn Project Manager, PI
Agronomist
<a href="mailto:labend@iastate.edu">labend@iastate.edu</a></p>

<p>Daryl Herzmann
Systems Administrator and Analyst
Agricultural Meteorologist
<a href="mailto:akrherz@iastate.edu">akrherz@iastsate.edu</a></p>

<p>Giorgi Chighladze
Data Manager and Analyst
Agricultural Engineer
<a href="mailto:gio@iastate.edu">gio@iastate.edu</a></p>

<p><img src="images/dt-160307.jpg" class="img img-responsive"></p>

EOF;
$t->render('single.phtml');
?>
