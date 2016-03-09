<?php 
/*
 * Make sure our user is authorized for this webpage
 */
function authorize(){
	$email = isset($_SERVER["REMOTE_USER"]) ?
			 strtolower($_SERVER["REMOTE_USER"]): null;
	if ($email == null){
		die("No REMOTE_USER set!");
	}
	$pgconn = pg_connect("dbname=sustainablecorn host=iemdb");
	$rs = pg_prepare($pgconn, "authorize",
			"SELECT email from website_users where email = $1");
	$rs = pg_execute($pgconn, "authorize", Array($email));
	if (pg_num_rows($rs) != 1){
		die("User '{$email}' not authorized for this website");
	}
}

authorize();
?>