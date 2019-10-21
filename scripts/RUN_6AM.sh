cd /opt/datateam/scripts/cscap

#python harvest_management.py
#python harvest_agronomic.py 2011
#python harvest_agronomic.py 2012
#python harvest_agronomic.py 2013
#python harvest_agronomic.py 2014
#python harvest_agronomic.py 2015

#python harvest_soil_nitrate.py 2011
#python harvest_soil_nitrate.py 2012
#python harvest_soil_nitrate.py 2013
#python harvest_soil_nitrate.py 2014
#python harvest_soil_nitrate.py 2015

#python harvest_soil_texture.py 2011
#python harvest_soil_texture.py 2012
#python harvest_soil_texture.py 2013
#python harvest_soil_texture.py 2014
#python harvest_soil_texture.py 2015

#python harvest_soil_bd.py 2011
#python harvest_soil_bd.py 2012
#python harvest_soil_bd.py 2013
#python harvest_soil_bd.py 2014
#python harvest_soil_bd.py 2015

#python harvest_soil_fertility.py

python email_daily_changes.py cscap
python email_daily_changes.py td
python email_daily_changes.py inrc
python email_daily_changes.py nutrinet
python email_daily_changes.py kb

#python ss2database.py

cd ../td
python harvest_management.py

cd ../auth
python drive2webaccess.py
