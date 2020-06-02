# Common settings used by the localhost HTTP and internet HTTPS vhost

  <FilesMatch \.html$>
    SetHandler application/x-httpd-php
  </FilesMatch>

  DocumentRoot "/opt/datateam/htdocs"
  <Directory "/opt/datateam/htdocs">
    # Need FollowSymLinks for mod_rewrite to work!
    Options Indexes FollowSymLinks
  </Directory>

  <Directory "/opt/datateam/htdocs/admin">
    SetEnv DATATEAM_APP admin

    AddHandler cgi-script .py
    Options +ExecCGI

  </Directory>

  <Directory "/opt/datateam/htdocs/cscap">
    SetEnv DATATEAM_APP cscap

    AddHandler cgi-script .py
    Options +ExecCGI

    RewriteEngine On
    RewriteRule ^decagon_(.*)_(.*)_(.*)_(.*)_(.*).js$ plot_decagon.py?site=$1&date=$2&days=$3&ptype=$4&depth=$5&view=js
    RewriteRule ^watertable_(.*)_(.*)_(.*)_(.*).js$ plot_watertable.py?site=$1&date=$2&days=$3&ptype=$4&view=js
    RewriteRule ^tileflow_(.*)_(.*)_(.*)_(.*).js$ plot_tileflow.py?site=$1&date=$2&days=$3&ptype=$4&view=js
    RewriteRule ^waterquality_(.*)_(.*)_(.*).js$ plot_waterquality.py?site=$1&varname=$2&ptype=$3&view=js
  </Directory>

  <Directory "/opt/datateam/htdocs/td">
    SetEnv DATATEAM_APP td

    # Default handler for python scripts
    WSGIProcessGroup iemwsgi_ap
    AddHandler wsgi-script .py
    Options +ExecCGI

    RewriteEngine On
    RewriteRule ^agronomic__(.*)__(.*)__(.*)__(.*).js$ plot_agronomic.py?site=$1&varname=$2&ptype=$3&view=js&group=$4
    RewriteRule ^decagon__(.*)__(.*)__(.*)__(.*)__(.*).js$ plot_decagon.py?site=$1&date=$2&days=$3&ptype=$4&depth=$5&view=js
    RewriteRule ^watertable__(.*)__(.*)__(.*)__(.*)_(.*).js$ plot_watertable.py?site=$1&date=$2&days=$3&ptype=$4&view=js&group=$5
    RewriteRule ^tileflow__(.*)__(.*)__(.*)__(.*)__([10]).js$ plot_tileflow.py?site=$1&date=$2&days=$3&ptype=$4&view=js&group=$5
    RewriteRule ^nitrateload__(.*)__(.*)__(.*)__(.*)__([10]).js$ plot_nitrateload.py?site=$1&date=$2&days=$3&by=$4&view=js&group=$5
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
  