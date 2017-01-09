-- Storage of Hourly Weather Observations
CREATE TABLE weather_hourly(
  siteid varchar(32),
  valid timestamptz,
  precip_mm real,
  srad_wm2 real,
  relhum_percent real,
  airtemp_c real,
  windspeed_mps real);
GRANT SELECT on weather_hourly to nobody,apache;
