-- Storage of Daily Weather
CREATE TABLE weather_daily(
  siteid varchar(32),
  valid date,
  precip_mm real,
  max_tmpc real,
  min_tmpc real,
  avg_smps real,
  srad_mj real);
CREATE INDEX weather_daily_idx on weather_daily(siteid, valid);
GRANT SELECT on weather_daily to nobody,apache;
