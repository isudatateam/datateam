-- The pain involved with poor schema decisions many moons ago
ALTER TABLE agronomic_data RENAME site to uniqueid;
ALTER TABLE agronomic_data_log RENAME site to uniqueid;

CREATE OR REPLACE FUNCTION agronomic_insert_before_F()
RETURNS TRIGGER
 AS $BODY$
DECLARE
    result INTEGER; 
BEGIN
    result = (select count(*) from agronomic_data
                where uniqueid = new.uniqueid and plotid = new.plotid and
                varname = new.varname and year = new.year and
                (value = new.value or (value is null and new.value is null))
               );

	-- Data is duplication, no-op
    IF result = 1 THEN
        RETURN null;
    END IF;

    result = (select count(*) from agronomic_data
                where uniqueid = new.uniqueid and plotid = new.plotid and
                varname = new.varname and year = new.year);

	-- Data is a new value!
    IF result = 1 THEN
    	UPDATE agronomic_data SET value = new.value, updated = now()
    	WHERE uniqueid = new.uniqueid and plotid = new.plotid and
                varname = new.varname and year = new.year;
        INSERT into agronomic_data_log SELECT * from agronomic_data WHERE
        		uniqueid = new.uniqueid and plotid = new.plotid and
                varname = new.varname and year = new.year;
        RETURN null;
    END IF;

    INSERT into agronomic_data_log (uniqueid, plotid, varname, year, value)
    VALUES (new.uniqueid, new.plotid, new.varname, new.year, new.value);

    -- The default branch is to return "NEW" which
    -- causes the original INSERT to go forward
    RETURN new;

END; $BODY$
LANGUAGE 'plpgsql' SECURITY DEFINER;
