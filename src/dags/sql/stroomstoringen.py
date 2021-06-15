# The data contains epoch times.
# In order to translate it to a timestamp, the data is updated.
# The dates are stored in millseconds.
# Unix timestamps measures time with seconds, and not milliseconds (in Postgres too)
# Therefore the values must be devided by 1000 before converting to timestamp
CONVERT_DATE_TIME = """
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_gemeld TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_gemeld/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_schatting TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_schatting/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_eind TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_eind/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN date_end TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(date_end/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_change TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_change/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_service_update TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_service_update/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN log_date TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(log_date/1000);
COMMIT;
"""
