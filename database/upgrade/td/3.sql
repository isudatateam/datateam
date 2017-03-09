-- Storage of Nitrate Loss Data
CREATE TABLE nitrateload_data(
  uniqueid varchar(24),
  plotid varchar(24),
  valid timestamptz,
  wat2 real,
  wat9 real,
  wat20 real,
  wat26 real);
CREATE INDEX nitrateload_data_idx on nitrateload_data(uniqueid, valid);
GRANT SELECT on nitrateload_data to nobody,apache;
