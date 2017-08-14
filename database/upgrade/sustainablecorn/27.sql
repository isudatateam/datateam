-- Add some indices, please!
CREATE INDEX soil_data_site_idx on soil_data(site);

CREATE INDEX ipm_data_uniqueid_idx on ipm_data(uniqueid);

CREATE INDEX ghg_data_uniqueid_idx on ghg_data(uniqueid);
