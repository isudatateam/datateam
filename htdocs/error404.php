<html>
<head>
</head>
<body>
<h3>File Not Found</h3>
</body>
</html>
<?php
$ref = isset($_SERVER["HTTP_REFERER"]) ? $_SERVER["HTTP_REFERER"] : 'none';
error_log("404 datateam:". $_SERVER["REQUEST_URI"]. ' referer: '. $ref);
?>