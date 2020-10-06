# In contrast to the .geojson source files, 
# the .csv source files only have a X en Y coordinate column: no geometry column
# Therefore the geometry column must be created (based upon the X and Y values)
# Before creating the column geometry, there is a check if a X and Y column exists
# Also een check is done on the existince of a value in X and Y (data may contain empty values)
# Finally the X and Y columns are dropped when the geometry column is created
ADD_GEOMETRY_COL = """
    DO $$DECLARE counter_x_y integer; 
    BEGIN
        SELECT count(1) into counter_x_y
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = '{{ params.tablename }}_new'
        AND column_name in ('x', 'y');

    if counter_x_y = 2 then
			
		ALTER TABLE {{ params.tablename }}_new DROP COLUMN IF EXISTS "geometry";
	  	
        ALTER TABLE {{ params.tablename }}_new ADD COLUMN IF NOT EXISTS "geometry" geometry(POINT, 28992) null;
        UPDATE {{ params.tablename }}_new  
        SET geometry = ST_Transform(ST_SetSRID(ST_MakePoint(REPLACE(x, ',', '.')::double precision, REPLACE(y, ',', '.')::double precision), 4326), 28992)
        WHERE 1=1
        AND length(x) > 0 and length(y) > 0;   
        DROP INDEX IF EXISTS {{ params.tablename }}_new_geom_idx; 
        CREATE INDEX {{ params.tablename }}_new_geom_idx ON {{ params.tablename }}_new USING GIST (geometry);

        ALTER TABLE {{ params.tablename }}_new DROP COLUMN IF EXISTS x;
        ALTER TABLE {{ params.tablename }}_new DROP COLUMN IF EXISTS y;
    
    end if;
    END $$;
"""

# The dataset zwembaden and sporthallen are present in one source file, to seperate the data
# the duplicate rows are delete from the table
DEL_ROWS = """    
    DELETE FROM sport_zwembad_new WHERE TYPE != 'Zwembad';
    COMMIT;
    DELETE FROM sport_sporthal_new WHERE TYPE != 'Sporthal';
    COMMIT;   
"""
