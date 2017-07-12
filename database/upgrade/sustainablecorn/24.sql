CREATE TABLE dwm(
 uniqueid varchar,
 plotid varchar,
 cropyear varchar,
 cashcrop varchar,
 boxstructure varchar,
 outletdepth varchar,
 outletdate date,
 comments varchar,
 updated varchar,
 editedby varchar
);
GRANT SELECT on dwm to nobody,apache;
GRANT ALL on dwm to mesonet;

CREATE TABLE notes(
 uniqueid varchar,
 calendaryear int,
 cropyear int,
 notes varchar,
 updated varchar,
 editedby varchar
);
GRANT SELECT on notes to nobody,apache;
GRANT ALL on notes to mesonet;