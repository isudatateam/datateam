cd /opt/datateam/scripts/cscap

python email_daily_changes.py inrc
python email_daily_changes.py nutrinet
python email_daily_changes.py kb
python email_daily_changes.py ilsoil
python email_daily_changes.py cig

cd ../auth
python drive2webaccess.py
