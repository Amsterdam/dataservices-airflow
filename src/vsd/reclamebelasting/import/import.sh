#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh


DS_FILENAME=reclame-tarief-gebieden
ZIP_FILE=$DS_FILENAME.zip
OBJECTSTORE_PATH=reclame/$ZIP_FILE

echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py $OBJECTSTORE_PATH
unzip $TMPDIR/$ZIP_FILE -d ${TMPDIR}

echo "Extracting reclamebelasting data"
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -nln reclamebelasting_new ${TMPDIR}/reclamebelasting.sql ${TMPDIR}/Reclame_tariefgebieden.shp

iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/reclamebelasting.sql > ${TMPDIR}/reclamebelasting.utf8.sql
# TODO: fix ID rename:
echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/reclamebelasting.utf8.sql
SQL

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;

ALTER TABLE IF EXISTS reclamebelasting RENAME TO reclamebelasting_old;
ALTER TABLE reclamebelasting_new RENAME TO reclamebelasting;
DROP TABLE IF EXISTS reclamebelasting_old;
ALTER INDEX reclamebelasting_new_pk RENAME TO reclamebelasting_pk;
ALTER INDEX reclamebelasting_new_wkb_geometry_geom_idx RENAME TO reclamebelasting_wkb_geometry_geom_idx;

COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
