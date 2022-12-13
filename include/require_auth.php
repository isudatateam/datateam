<?php
/*
 * Make sure our user is authorized for this webpage
 */
function authorize()
{
    $email = isset($_SERVER["REMOTE_USER"]) ?
        strtolower($_SERVER["REMOTE_USER"]) : null;
    if ($email == null) {
        die("No REMOTE_USER set!");
    }
    // This is set via local .htaccess files, hacky
    $app = $_SERVER["DATATEAM_APP"];
    $pgconn = pg_connect("dbname=sustainablecorn host=iemdb-sustainablecorn.local user=nobody");
    $rs = pg_prepare(
        $pgconn,
        "authorize",
        "SELECT email from website_users u JOIN website_access_levels l "
            . "on (u.access_level = l.access_level) where email = $1"
            . "and l.appid = $2"
    );
    $rs = pg_execute($pgconn, "authorize", array($email, $app));
    if (pg_num_rows($rs) != 1) {
        die("User '{$email}' not authorized for this website");
    }
    // Can this user admin?
    $rs = pg_execute($pgconn, "authorize", array($email, "admin"));
    if (pg_num_rows($rs) == 1) {
        $_SERVER["DATATEAM_ADMIN"] = True;
    }
    // Log the access
    $rs = pg_prepare(
        $pgconn,
        "LOGIT",
        "UPDATE website_users SET last_usage = now() WHERE "
            . "email = $1 and access_level = "
            . "(SELECT access_level from website_access_levels where appid = $2)"
    );
    $rs = pg_execute($pgconn, "LOGIT", array($email, $app));
}

if ($_SERVER["REMOTE_ADDR"] != "127.0.0.1") {
    authorize();
}
