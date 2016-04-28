-- Storage of website edits metadata
CREATE TABLE website_edits(
  username varchar(128),
  edit_ts timestamptz DEFAULT now(),
  edit_table varchar(64),
  uniqueid varchar,
  plotid varchar,
  valid timestamptz,
  edit_column varchar(64),
  newvalue real,
  comment text);
GRANT ALL on website_edits to nobody,apache;

-- Allow editing
GRANT ALL on tileflow_data to nobody,apache;
GRANT ALL on decagon_data to nobody,apache;
GRANT ALL on watertable_data to nobody,apache;
