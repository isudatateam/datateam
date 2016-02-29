-- Add more vars to weather tables
ALTER TABLE weather_data_obs ADD precip real;
ALTER TABLE weather_data_obs ADD srad real;

ALTER TABLE weather_data_daily ADD sknt real;