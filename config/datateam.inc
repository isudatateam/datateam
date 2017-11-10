# Common settings used by the localhost HTTP and internet HTTPS vhost

  <FilesMatch \.html$>
    SetHandler application/x-httpd-php
  </FilesMatch>

  DocumentRoot "/opt/datateam/htdocs"
  <Directory "/opt/datateam/htdocs">
    # Need FollowSymLinks for mod_rewrite to work!
    Options Indexes FollowSymLinks
    AllowOverride All
    Order allow,deny
    Allow from all
  </Directory>
  
  Alias /usage "/mesonet/share/usage/datateam.agron.iastate.edu/"
  <Directory "/mesonet/share/usage/datateam.agron.iastate.edu/">
    Options Indexes MultiViews
    AllowOverride None
    Order allow,deny
    Allow from all
  </Directory>
  
  # Since RHEL Apache does not log 404s, sigh
  ErrorDocument 404 /http_errors/error404.php
  # ErrorDocument 401 /error404.php

  <Location "/http_errors/">
    # https://bz.apache.org/bugzilla/show_bug.cgi?id=55415
    ProxyErrorOverride Off
  </Location>
  