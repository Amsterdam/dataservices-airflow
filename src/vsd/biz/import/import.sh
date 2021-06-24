#!/usr/bin/env bash

# export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# export PYTHONPATH="$SCRIPT_DIR/../..:$SCRIPT_DIR/../../.."
# export SHARED_DIR=${SCRIPT_DIR}/../../shared

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"

ogr2ogr -f "PGDump" ${TMPDIR}/BIZZONES.sql ${DATA_DIR}/BIZZONES.shp
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/BIZZONES.sql > ${TMPDIR}/BIZZONES.utf8.sql
python ${SCRIPT_DIR}/convert_data.py ${TMPDIR}/BIZZONES.utf8.sql ${DATA_DIR}/BIZ\ Dataset\ 2020-01014.xlsx ${TMPDIR}/biz_data_insert.sql

echo "Create tables"
psql -X --set ON_ERROR_STOP=on << SQL
\i ${SCRIPT_DIR}/biz_data_create.sql
SQL

echo "Import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/biz_data_insert.sql
SQL

${SCRIPT_DIR}/check_imported_biz_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS biz_data RENAME TO biz_data_old;
ALTER TABLE biz_data_new RENAME TO biz_data;
DROP TABLE IF EXISTS biz_data_old CASCADE;
ALTER VIEW biz_view_new RENAME TO biz_view;
ALTER INDEX naam_unique_new RENAME TO naam_unique;
ALTER INDEX biz_data_new_wkb_geometry_geom_idx RENAME TO biz_data_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
