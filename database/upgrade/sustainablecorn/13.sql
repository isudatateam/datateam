-- Finer grain permissions
CREATE TABLE website_access_levels(
  access_level smallint UNIQUE NOT NULL,
  appid varchar,
  label varchar);
GRANT SELECT on website_access_levels to nobody,apache;
INSERT into website_access_levels VALUES (0, 'admin', 'Administrators');
INSERT into website_access_levels VALUES (1, 'cscap', 'Sustainable Corn CAP');
INSERT into website_access_levels VALUES (2, 'td', 'Transforming Drainage');

ALTER TABLE website_users ADD CONSTRAINT distfk FOREIGN KEY (access_level)
  REFERENCES website_access_levels(access_level);

ALTER TABLE website_users DROP CONSTRAINT website_users_email_key;
