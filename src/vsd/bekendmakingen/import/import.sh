#!/usr/bin/env bash

source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

wget "http://geozet.koop.overheid.nl/wfs?request=GetFeature&SERVICE=WFS&VERSION=1.2.0&TYPENAME=geozet:bekendmakingen_punt&FILTER=%3CFilter%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3Egeozet:overheid%3C/PropertyName%3E%3CLiteral%3EAmsterdam%3C/Literal%3E%3C/PropertyIsEqualTo%3E%3C/Filter%3E&outputFormat=application/json" -O ${TMPDIR}/bekendmakingen.json

ogr2ogr -f "PGDump" -a_srs EPSG:28992 -nln bekendmakingen_new ${TMPDIR}/bekendmakingen.sql ${TMPDIR}/bekendmakingen.json

echo "Create tables & import data for bekendmakingen"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS bekendmakingen_new CASCADE;
COMMIT;
\i ${TMPDIR}/bekendmakingen.sql
BEGIN;
ALTER TABLE bekendmakingen_new DROP column bbox;
COMMIT;
SQL

${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
ALTER TABLE IF EXISTS bekendmakingen RENAME TO bekendmakingen_old;
ALTER TABLE bekendmakingen_new RENAME TO bekendmakingen;
DROP TABLE IF EXISTS bekendmakingen_old CASCADE;
ALTER INDEX bekendmakingen_new_pk RENAME TO bekendmakingen_pk;
ALTER INDEX bekendmakingen_new_wkb_geometry_geom_idx RENAME TO bekendmakingen_wkb_geometry_geom_idx;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh
