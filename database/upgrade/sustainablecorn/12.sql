
CREATE OR REPLACE FUNCTION soil_insert_before_F()
RETURNS TRIGGER
 AS $BODY$
DECLARE
    result INTEGER; 
BEGIN
    result = (select count(*) from soil_data
                where site = new.site and plotid = new.plotid and
                varname = new.varname and year = new.year and
                depth = new.depth and subsample = new.subsample and 
                (value = new.value or (value is null and new.value is null)) and
                (sampledate = new.sampledate or (sampledate is null and new.sampledate is null))
               );

	-- Data is duplication, no-op
    IF result = 1 THEN
        RETURN null;
    END IF;

    result = (select count(*) from soil_data
                where site = new.site and plotid = new.plotid and
                varname = new.varname and year = new.year
                and depth = new.depth and subsample = new.subsample and
                (sampledate = new.sampledate or (sampledate is null and new.sampledate is null))
                );

	-- Data is a new value!
    IF result = 1 THEN
    	UPDATE soil_data SET value = new.value, updated = now()
    	WHERE site = new.site and plotid = new.plotid and
                varname = new.varname and year = new.year and
                depth = new.depth and subsample = new.subsample and
                (sampledate = new.sampledate or (sampledate is null and new.sampledate is null));
        INSERT into soil_data_log SELECT * from soil_data WHERE
        		site = new.site and plotid = new.plotid and
                varname = new.varname and year = new.year and depth = new.depth
                and subsample = new.subsample and
                (sampledate = new.sampledate or (sampledate is null and new.sampledate is null));
        RETURN null;
    END IF;

    INSERT into soil_data_log (site, plotid, varname, year, depth, subsample, value, sampledate)
    VALUES (new.site, new.plotid, new.varname, new.year, new.depth, new.subsample, new.value,
    	new.sampledate);

    -- The default branch is to return "NEW" which
    -- causes the original INSERT to go forward
    RETURN new;

END; $BODY$
LANGUAGE 'plpgsql' SECURITY DEFINER;

DROP INDEX soil_data_idx;
CREATE UNIQUE index soil_data_idx on 
	soil_data(site, plotid, varname, year, depth, subsample, sampledate);
