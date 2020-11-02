# Importing the geojson by OGR2OGR does not auto detect a date datatype, so the conversion of datatypes is done in SQL
SET_DATE_DATATYPE = """
    ALTER TABLE {{ params.tablename }} ALTER COLUMN startdatum TYPE date USING startdatum::date;
    ALTER TABLE {{ params.tablename }} ALTER COLUMN einddatum TYPE date USING startdatum::date;
"""
