-- Storage of Water Quality Data
CREATE TABLE waterquality_data(
  uniqueid varchar(24),
  plotid varchar(24),
  valid timestamp with time zone,
  sample_type varchar(32),
  varname varchar(8),
  value real);

GRANT ALL on waterquality_data to mesonet,ldm;
GRANT SELECT on waterquality_data to nobody,apache;
