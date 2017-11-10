<html>
<head>
</head>
<body>
<h3>File Not Found</h3>
</body>
</html>
<?php
$ref = isset($_SERVER["HTTP_REFERER"]) ? $_SERVER["HTTP_REFERER"] : 'none';
openlog("datateam", LOG_PID | LOG_PERROR, LOG_LOCAL1);
syslog(LOG_WARNING, "404 ". $_SERVER["REQUEST_URI"] .
    ' remote: '. $_SERVER["REMOTE_ADDR"] .
    ' referer: '. $ref);

?>