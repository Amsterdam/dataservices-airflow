#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

# ENVIRONMENT=${DATAPUNT_ENVIRONMENT:-acceptance}
ENVIRONMENT=acceptance

DS_FILENAME=bb_quotum.sql
OBJECTSTORE_PATH=bed_and_breakfast/${ENVIRONMENT}/${DS_FILENAME}

echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py "$OBJECTSTORE_PATH"

egrep -v "^ALTER TABLE.*OWNER TO" ${TMPDIR}/${ENVIRONMENT}/${DS_FILENAME} | egrep -v "^GRANT SELECT ON" > ${TMPDIR}/bb_quotum_new.sql
perl -pi -e "s/quota_bbkaartlaagexport/bb_quotum_new/g" ${TMPDIR}/bb_quotum_new.sql

psql -X --set ON_ERROR_STOP=on << SQL
BEGIN;
DROP TABLE IF EXISTS bb_quotum_new CASCADE;
DROP SEQUENCE IF EXISTS bb_quotum_new_id_seq CASCADE;
DROP INDEX IF EXISTS bb_quotum_new_geo_id;
\i ${TMPDIR}/bb_quotum_new.sql;
COMMIT;
SQL

${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS bb_quotum RENAME TO bb_quotum_old;
ALTER SEQUENCE IF EXISTS bb_quotum_id_seq RENAME TO bb_quotum_old_id_seq;
ALTER TABLE bb_quotum_new RENAME TO bb_quotum;
ALTER SEQUENCE bb_quotum_new_id_seq RENAME TO bb_quotum_id_seq;
DROP TABLE IF EXISTS bb_quotum_old;
DROP SEQUENCE IF EXISTS bb_quotum_old_id_seq CASCADE;
ALTER INDEX bb_quotum_new_pkey RENAME TO bb_quotum_pkey;
ALTER INDEX bb_quotum_new_geo_id RENAME TO bb_quotum_geo_id;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
