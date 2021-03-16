# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# Ogr2ogr demands a numeric-like column as identifier and also makes
# the datatype default to serial. A serial datatype causes to use
# a database sequence which has a default output size limit.
# Any value greater then the limit size, will result in an error.
# Therefor the temporary identifier is dropped (==fid) and
# the real identifier is set to be the primairy key after
# ogr2ogr has loaded the data.
SQL_REDEFINE_PK = """
    ALTER TABLE {{ params.tablename }} DROP COLUMN FID;
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT {{ params.tablename }}_pkey PRIMARY KEY (ID);
"""
