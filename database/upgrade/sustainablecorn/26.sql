-- Storage of downloads
CREATE TABLE website_downloads(
  email varchar,
  valid timestamptz default now()
);
GRANT ALL on website_downloads to nobody,apache,ldm,mesonet;
