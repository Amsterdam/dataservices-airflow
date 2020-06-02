#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh


DS_FILENAME=VuurwerkVrijeZones
ZIP_FILE=$DS_FILENAME.zip
OBJECTSTORE_PATH=vuurwerk/$ZIP_FILE

echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py $OBJECTSTORE_PATH
unzip $TMPDIR/$ZIP_FILE -d ${TMPDIR}

echo "Extracting vuurwerk data"
ogr2ogr -f "PGDump" -t_srs EPSG:28992 -nln vuurwerk_new ${TMPDIR}/vuurwerk.sql ${TMPDIR}/VVZ_gebieden_totaal.shp

iconv -f iso-8859-1 -t utf-8  ${TMPDIR}/vuurwerk.sql > ${TMPDIR}/vuurwerk.utf8.sql
# TODO: fix ID rename:
echo "Create tables & import data"
psql -X --set ON_ERROR_STOP=on <<SQL
\i ${TMPDIR}/vuurwerk.utf8.sql
SQL

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;

ALTER TABLE IF EXISTS vuurwerk RENAME TO vuurwerk_old;
ALTER TABLE vuurwerk_new RENAME TO vuurwerk;
DROP TABLE IF EXISTS vuurwerk_old;
ALTER INDEX vuurwerk_new_pk RENAME TO vuurwerk_pk;
ALTER INDEX vuurwerk_new_wkb_geometry_geom_idx RENAME TO vuurwerk_wkb_geometry_geom_idx;

COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
