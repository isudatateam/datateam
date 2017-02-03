-- Storage of Soil Moisture
-- Add Decagon Data Storage
CREATE TABLE decagon_data(
	uniqueid varchar(24),
	plotid varchar(24),
	valid timestamptz,
	d1moisture real,
	d1moisture_qcflag char(1),
	d1moisture_qc real,
	d1temp real,
	d1temp_qcflag char(1),
	d1temp_qc real,
	d1ec real,
	d1ec_qcflag char(1),
	d1ec_qc real,
	d2moisture real,
	d2moisture_qcflag char(1),
	d2moisture_qc real,
	d2temp real,
	d2temp_qcflag char(1),
	d2temp_qc real,
	d3moisture real,
	d3moisture_qcflag char(1),
	d3moisture_qc real,
	d3temp real,
	d3temp_qcflag char(1),
	d3temp_qc real,
	d4moisture real,
	d4moisture_qcflag char(1),
	d4moisture_qc real,
	d4temp real,
	d4temp_qcflag char(1),
	d4temp_qc real,
	d5moisture real,
	d5moisture_qcflag char(1),
	d5moisture_qc real,
	d5temp real,
	d5temp_qcflag char(1),
	d5temp_qc real
);
CREATE INDEX decagon_valid_idx on decagon_data(valid);
GRANT ALL on decagon_data to nobody,apache;
create index decagon_data_idx on decagon_data(uniqueid, plotid);
