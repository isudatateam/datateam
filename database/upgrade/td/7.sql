CREATE TABLE plant_harvest(
updated varchar,
plantrateunits varchar,
cropyear varchar,
planthybrid varchar,
comments varchar,
plantrate varchar,
calendaryear varchar,
uniqueid varchar,
plantmaturity varchar,
valid date,
editedby varchar,
operation varchar
);
GRANT ALL on plant_harvest to mesonet;
GRANT SELECT on plant_harvest to nobody,apache;

CREATE TABLE soil_fert(
stabilizername varchar,
zinc varchar,
stabilizerused varchar,
magnesiumelem varchar,
manuremethod varchar,
productrate varchar,
updated varchar,
calendaryear varchar,
nitrogen varchar,
sulfur varchar,
operation varchar,
manurerate varchar,
comments varchar,
nitrogenelem varchar,
limerate varchar,
fertilizerform varchar,
manurecomposition varchar,
potash varchar,
sulfurelem varchar,
stabilizer varchar,
manurerateunits varchar,
cropyear varchar,
potassium varchar,
ironelem varchar,
calciumelem varchar,
fertilizerformulation varchar,
editedby varchar,
uniqueid varchar,
manuresource varchar,
valid date,
phosphorus varchar,
fertilizercrop varchar,
fertilizerapptype varchar,
phosphoruselem varchar,
zincelem varchar,
calcium varchar,
depth varchar,
phosphate varchar,
magnesium varchar,
iron varchar,
potassiumelem varchar
);
GRANT ALL on soil_fert to mesonet;
GRANT select on soil_fert to nobody,apache;

CREATE TABLE pesticides(
crop varchar,
valid date,
calendaryear varchar,
operation varchar,
comments varchar,
adjuvant varchar,
product4 varchar,
product3 varchar,
product2 varchar,
product1 varchar,
method varchar,
updated varchar,
cropyear varchar,
totalrate varchar,
editedby varchar,
uniqueid varchar,
timing varchar,
rate4 varchar,
rate3 varchar,
rate2 varchar,
rate1 varchar,
rateunit4 varchar,
rateunit1 varchar,
rateunit3 varchar,
rateunit2 varchar);
GRANT ALL on pesticides to mesonet;
GRANT select on pesticides to nobody,apache;

CREATE TABLE residue_mngt(
updated varchar,
residueplantingpercentage varchar,
notill varchar,
cropyear varchar,
comments varchar,
editedby varchar,
calendaryear varchar,
uniqueid varchar);
GRANT ALL on residue_mngt to mesonet;
GRANT select on residue_mngt to nobody,apache;

CREATE TABLE dwm(
updated varchar,
cropyear varchar,
outletdate varchar,
comments varchar,
outletdepth varchar,
boxstructure varchar,
editedby varchar,
calendaryear varchar,
uniqueid varchar);
GRANT ALL on dwm to mesonet;
GRANT select on dwm to nobody,apache;

CREATE TABLE irrigation(
updated varchar,
irrigationmethod varchar,
cropyear varchar,
irrigationamount varchar,
comments varchar,
editedby varchar,
calendaryear varchar,
uniqueid varchar,
irrstartdate varchar,
irrstructure varchar,
irrenddate varchar);
GRANT ALL on irrigation to mesonet;
GRANT select on irrigation to nobody,apache;

CREATE TABLE notes(
updated varchar,
cropyear varchar,
notes varchar,
editedby varchar,
calendaryear varchar,
uniqueid varchar);
GRANT ALL on notes to mesonet;
GRANT select on notes to nobody,apache;
