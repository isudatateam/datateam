<?php 
require_once "../include/myview.php";
$t = new MyView();
$t->title = "ISU Data Team!";
$t->content = <<<EOM
<p>&nbsp;</p>

<h3>ISU Data Team Contacts</h3>

<p>Questions regarding the research database such as experimental designs, farm management operations, 
research and metadata collected, and protocols should be directed to 
<a href="mailto:isudatateam@iastate.edu">isudatateam@iastate.edu</a>. 
The team members listed below will answer your questions. We will determine if contacting the individual 
research site faculty and staff members are necessary.</p>

<p>Lori Abendroth<br />
Principal Investigator (2011-2020)<br />
Research Agronomist (current)<br />
USDA ARS, Cropping Systems and Water Quality Unit<br />
<a href="mailto:lori.abendroth@usda.gov">lori.abendroth@usda.gov</a></p>

<p>Daryl Herzmann<br>
Systems Administrator and Analyst<br>
Agricultural Meteorologist<br>
Iowa State University<br />
<a href="mailto:akrherz@iastate.edu">akrherz@iastate.edu</a></p>

<p>Giorgi Chighladze<br>
Data Manager and Analyst<br>
Agricultural Engineer<br>
Iowa State University<br />
<a href="mailto:gio@iastate.edu">gio@iastate.edu</a></p>

<p>
Appreciation is extended to Stephanie Bowden, Suresh Lokhande, and 
Katie Schwaegler for their contributions in management and review of data, 
metadata, photographs, and maps.
</p>

<p>&nbsp<p>

<p><img src="images/dt-160307.jpg" class="img img-responsive"></p>

EOM;
$t->render('single.phtml');
