-- Updates made to pesticides table
ALTER TABLE pesticides RENAME adjuvant to adjuvant1;
ALTER TABLE pesticides ADD adjuvant2 varchar;
