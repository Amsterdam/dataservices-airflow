#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Download files from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py "grootstedelijkegebieden/GBD_grootstedelijke_projecten.prj"
python $SHARED_DIR/utils/get_objectstore_file.py "grootstedelijkegebieden/GBD_grootstedelijke_projecten.dbf"
python $SHARED_DIR/utils/get_objectstore_file.py "grootstedelijkegebieden/GBD_grootstedelijke_projecten.shx"
python $SHARED_DIR/utils/get_objectstore_file.py "grootstedelijkegebieden/GBD_grootstedelijke_projecten.shp"

ogr2ogr -f "PGDump" -nlt MULTIPOLYGON -nln gbd_grootstedelijke_projecten_new ${TMPDIR}/gbd_grootstedelijke_projecten.sql ${TMPDIR}/GBD_grootstedelijke_projecten.shp

iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/gbd_grootstedelijke_projecten.sql > ${TMPDIR}/gbd_grootstedelijke_projecten.utf8.sql

echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/gbd_grootstedelijke_projecten.utf8.sql
SQL

echo "Check imported data"
${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS gbd_grootstedelijke_projecten;
ALTER TABLE IF EXISTS gbd_grootstedelijke_projecten_new RENAME TO gbd_grootstedelijke_projecten;
ALTER INDEX gbd_grootstedelijke_projecten_new_pk RENAME TO gbd_grootstedelijke_projecten_pk;
ALTER INDEX gbd_grootstedelijke_projecten_new_wkb_geometry_geom_idx RENAME TO gbd_grootstedelijke_projecten_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
