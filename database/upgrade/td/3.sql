-- Storage of Nitrate Loss Data
CREATE TABLE nitrateloss_data(
  uniqueid varchar(24),
  plotid varchar(24),
  valid timestamptz,
  wat2 real,
  wat9 real,
  wat20 real,
  wat26 real);
CREATE INDEX nitrateloss_data_idx on nitrateloss_data(uniqueid, valid);
GRANT SELECT on nitrateloss_data to nobody,apache;
