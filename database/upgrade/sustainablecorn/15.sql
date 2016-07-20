-- Fix names of columns
ALTER TABLE decagon_data RENAME d1moisture_flag to d1moisture_qcflag;
ALTER TABLE decagon_data RENAME d2moisture_flag to d2moisture_qcflag;
ALTER TABLE decagon_data RENAME d3moisture_flag to d3moisture_qcflag;
ALTER TABLE decagon_data RENAME d4moisture_flag to d4moisture_qcflag;
ALTER TABLE decagon_data RENAME d5moisture_flag to d5moisture_qcflag;

ALTER TABLE decagon_data RENAME d1temp_flag to d1temp_qcflag;
ALTER TABLE decagon_data RENAME d2temp_flag to d2temp_qcflag;
ALTER TABLE decagon_data RENAME d3temp_flag to d3temp_qcflag;
ALTER TABLE decagon_data RENAME d4temp_flag to d4temp_qcflag;
ALTER TABLE decagon_data RENAME d5temp_flag to d5temp_qcflag;

ALTER TABLE decagon_data RENAME d1ec_flag to d1ec_qcflag;
