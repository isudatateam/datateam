-- Storage of authorized Google OpenID users
CREATE TABLE website_users(
  email varchar NOT NULL UNIQUE,
  last_usage timestamptz,
  access_level smallint);
GRANT ALL on website_users to nobody,apache;
