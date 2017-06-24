-- Storage of IPM
CREATE TABLE ipm_data(
  uniqueid varchar(32),
  plotid varchar(32),
  year smallint,
  date date,
  ipm01 varchar,
  ipm02 varchar,
  ipm03 varchar,
  ipm04 varchar,
  ipm05 varchar,
  ipm06 varchar,
  ipm07 varchar,
  ipm08 varchar,
  ipm09 varchar,
  ipm10 varchar,
  ipm11 varchar,
  ipm12 varchar,
  ipm13 varchar,
  ipm14 varchar);
GRANT SELECT on ipm_data to nobody,apache;
GRANT ALL on ipm_data to mesonet,ldm;