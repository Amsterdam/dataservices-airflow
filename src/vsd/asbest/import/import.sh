#!/usr/bin/env bash

# export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# export PYTHONPATH="$SCRIPT_DIR/../..:$SCRIPT_DIR/../../.."
# export SHARED_DIR=${SCRIPT_DIR}/../../shared

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh


DS_FILENAME=Amsterdam_Asbestverdachte_daken_Shape
ZIP_FILE=$DS_FILENAME.zip
OBJECTSTORE_PATH=asbest/$ZIP_FILE

echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py $OBJECTSTORE_PATH
unzip $TMPDIR/$ZIP_FILE -d ${TMPDIR}

echo "Extracting daken data"
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -nln asbest_daken_new ${TMPDIR}/asbest_daken.sql ${TMPDIR}/Shape/Asbestverdachte_daken_gegevens.shp

echo "Extracting percelen data"
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -nln asbest_percelen_new ${TMPDIR}/asbest_percelen.sql ${TMPDIR}/Shape/Asbestverdachte_Percelen.shp


iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/asbest_daken.sql > ${TMPDIR}/asbest_daken.utf8.sql
iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/asbest_percelen.sql > ${TMPDIR}/asbest_percelen.utf8.sql

echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/asbest_daken.utf8.sql
ALTER TABLE asbest_daken_new RENAME COLUMN identifica TO pandidentificatie;

\i ${TMPDIR}/asbest_percelen.utf8.sql
SQL

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS asbest_daken RENAME TO asbest_daken_old;
ALTER TABLE asbest_daken_new RENAME TO asbest_daken;
DROP TABLE IF EXISTS asbest_daken_old;
ALTER INDEX asbest_daken_new_pk RENAME TO asbest_daken_pk;
ALTER INDEX asbest_daken_new_wkb_geometry_geom_idx RENAME TO asbest_daken_wkb_geometry_geom_idx;

ALTER TABLE IF EXISTS asbest_percelen RENAME TO asbest_percelen_old;
ALTER TABLE asbest_percelen_new RENAME TO asbest_percelen;
DROP TABLE IF EXISTS asbest_percelen_old;
ALTER INDEX asbest_percelen_new_pk RENAME TO asbest_percelen_pk;
ALTER INDEX asbest_percelen_new_wkb_geometry_geom_idx RENAME TO asbest_percelen_wkb_geometry_geom_idx;

COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh

