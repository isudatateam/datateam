-- Storage of GHG Data
CREATE TABLE ghg_data(
  uniqueid varchar(32),
  plotid varchar(32),
  year int,
  date date,
  ghg01 varchar(64),
  ghg02 varchar(64),
  ghg03 real,
  ghg04 real,
  ghg05 real,
  ghg06 real,
  ghg07 real,
  ghg08 real,
  ghg09 real,
  ghg10 real,
  ghg11 real,
  ghg12 real,
  ghg13 real,
  ghg14 real,
  ghg15 real,
  ghg16 real);
GRANT SELECT on ghg_data to nobody,apache;
GRANT ALL on ghg_data to mesonet,ldm;
