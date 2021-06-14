# The data contains epoch times.
# In order to translate it to a timestamp, the data is updated.
CONVERT_DATE_TIME = """
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_gemeld TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_schatting TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_eind TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN date_end TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_change TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_service_update TYPE varchar(50);
ALTER TABLE {{ params.tablename }} ALTER COLUMN log_date TYPE varchar(50);

UPDATE {{ params.tablename }}
SET storing_datum_gemeld = to_timestamp(rpad(storing_datum_gemeld, 13, '0')::bigint / 1000),
    storing_datum_schatting = to_timestamp(rpad(storing_datum_schatting, 13, '0')::bigint / 1000),
    storing_datum_eind = to_timestamp(rpad(storing_datum_eind, 13, '0')::bigint / 1000),
    date_end = to_timestamp(rpad(date_end, 13, '0')::bigint / 1000),
    storing_datum_change = to_timestamp(rpad(storing_datum_change, 13, '0')::bigint / 1000),
    storing_service_update = to_timestamp(rpad(storing_service_update, 13, '0')::bigint / 1000),
    log_date = to_timestamp(rpad(log_date, 13, '0')::bigint / 1000)
WHERE 1=1;
COMMIT;

ALTER TABLE {{ params.tablename }} ADD COLUMN storing_datum_gemeld_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET storing_datum_gemeld_tmp = storing_datum_gemeld::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_gemeld TYPE TIMESTAMP
    without time zone USING storing_datum_gemeld_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN storing_datum_gemeld_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN storing_datum_schatting_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET storing_datum_schatting_tmp = storing_datum_schatting::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_schatting TYPE TIMESTAMP
    without time zone USING storing_datum_schatting_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN storing_datum_schatting_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN storing_datum_eind_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET storing_datum_eind_tmp = storing_datum_eind::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_eind TYPE TIMESTAMP
    without time zone USING storing_datum_eind_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN storing_datum_eind_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN date_end_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET date_end_tmp = date_end::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN date_end TYPE TIMESTAMP
    without time zone USING date_end_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN date_end_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN storing_datum_change_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET storing_datum_change_tmp = storing_datum_change::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_change TYPE TIMESTAMP
    without time zone USING storing_datum_change_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN storing_datum_change_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN storing_service_update_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET storing_service_update_tmp = storing_service_update::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_service_update TYPE TIMESTAMP
    without time zone USING storing_service_update_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN storing_service_update_tmp;

ALTER TABLE {{ params.tablename }} ADD COLUMN log_date_tmp TIMESTAMP
    without time zone NULL;
UPDATE {{ params.tablename }} SET log_date_tmp = log_date::TIMESTAMP;
ALTER TABLE {{ params.tablename }} ALTER COLUMN log_date TYPE TIMESTAMP
    without time zone USING log_date_tmp;
ALTER TABLE {{ params.tablename }} DROP COLUMN log_date_tmp;
"""
