#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data for milieuzones"

python ${SCRIPT_DIR}/json2geojson.py ${DATA_DIR}/milieuzones.json ${TMPDIR}/milieuzones.geo.json
ogr2ogr -f "PGDump"  -t_srs EPSG:28992 -s_srs EPSG:4326 -nln milieuzones_new ${TMPDIR}/milieuzones.sql ${TMPDIR}/milieuzones.geo.json

echo "Create tables & import data for milieuzones"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/milieuzones.sql
ALTER TABLE milieuzones_new ADD COLUMN vanafdatum_new DATE;
UPDATE milieuzones_new SET vanafdatum_new = to_date(vanafdatum, 'DD-MM-YYYY');
ALTER TABLE milieuzones_new DROP COLUMN vanafdatum;
ALTER TABLE milieuzones_new RENAME COLUMN vanafdatum_new TO vanafdatum;
ALTER TABLE milieuzones_new ADD COLUMN color VARCHAR(7);
UPDATE milieuzones_new SET color = c.color FROM
  ( VALUES('vracht', '#772b90'), ('bestel', '#f7f706'),
          ('taxi','#ec008c'), ( 'brom- en snorfiets', '#3062b7'),
          ('touringcar', '#fa9f1b')) AS c(verkeerstype, color)
  WHERE milieuzones_new.verkeerstype = c. verkeerstype;
SQL

echo "Check imported data for milieuzones"
${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS milieuzones RENAME TO milieuzones_old;
ALTER TABLE milieuzones_new RENAME TO milieuzones;
DROP TABLE IF EXISTS milieuzones_old;
ALTER INDEX milieuzones_new_pk RENAME TO milieuzones_pk;
ALTER INDEX milieuzones_new_wkb_geometry_geom_idx RENAME TO milieuzones_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
