-- Storage of whatever weather observations we have
CREATE TABLE weather_observations(
  siteid varchar(32),
  valid timestamptz,
  precip_mm real,
  srad_wm2 real,
  relhum_percent real,
  airtemp_c real,
  windspeed_mps real,
  srad_wms real,
  winddir_deg real,
  windgust_mps real);
GRANT SELECT on weather_observations to nobody,apache;
