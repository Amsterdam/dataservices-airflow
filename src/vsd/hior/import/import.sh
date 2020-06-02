#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"
wget -O "${TMPDIR}/HIOR Amsterdam.xlsx" "http://131f4363709c46b89a6ba5bc764b38b9.objectstore.eu/hior/HIOR Amsterdam.xlsx"
python ${SCRIPT_DIR}/import_hior.py "${TMPDIR}/HIOR Amsterdam.xlsx" ${TMPDIR}

echo "Create tables"
psql -X --set ON_ERROR_STOP=on << SQL
\i ${SCRIPT_DIR}/hior_data_create.sql
SQL

echo "Import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/hior_items_new.sql
\i ${TMPDIR}/hior_properties_new.sql
\i ${TMPDIR}/hior_attributes_new.sql
\i ${TMPDIR}/hior_faq_new.sql
\i ${TMPDIR}/hior_metadata_new.sql
SQL

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS hior_items RENAME TO hior_items_old;
ALTER TABLE IF EXISTS hior_properties RENAME TO hior_properties_old;
ALTER TABLE IF EXISTS hior_attributes RENAME TO hior_attributes_old;
ALTER TABLE IF EXISTS hior_faq RENAME TO hior_faq_old;
ALTER TABLE IF EXISTS hior_metadata RENAME TO hior_metadata_old;
ALTER TABLE hior_items_new RENAME TO hior_items;
ALTER TABLE hior_properties_new RENAME TO hior_properties;
ALTER TABLE hior_attributes_new RENAME TO hior_attributes;
ALTER TABLE hior_faq_new RENAME TO hior_faq;
ALTER TABLE hior_metadata_new RENAME TO hior_metadata;
DROP TABLE IF EXISTS hior_properties_old CASCADE;
DROP TABLE IF EXISTS hior_attributes_old CASCADE;
DROP TABLE IF EXISTS hior_items_old CASCADE;
DROP TABLE IF EXISTS hior_faq_old CASCADE;
DROP TABLE IF EXISTS hior_metadata_old CASCADE;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
