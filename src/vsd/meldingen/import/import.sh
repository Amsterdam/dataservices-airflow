#!/usr/bin/env bash
set -e
source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

ENVIRONMENT=${DATAPUNT_ENVIRONMENT:-acceptance}

echo "Process import data"
ZIPFILE=mora.zip
OBJECTSTORE_PATH=Dataservices/meldingen/${ENVIRONMENT}/$ZIPFILE

mkdir -p ${TMPDIR}/meldingen
echo "Download file from objectstore"
python $SHARED_DIR/utils/get_objectstore_file.py "$OBJECTSTORE_PATH" -s objectstore_dataservices

unzip $TMPDIR/meldingen/${ENVIRONMENT}/$ZIPFILE -d ${TMPDIR}

psql -X --set ON_ERROR_STOP=on << SQL
DROP TABLE IF EXISTS pte.mora_melding_dag_buurt_categorie CASCADE;
CREATE SCHEMA IF NOT EXISTS pte;
\i ${TMPDIR}/mora.backup
SQL

psql -X --set ON_ERROR_STOP=on << SQL
CREATE SEQUENCE IF NOT EXISTS public.meldingen_statistiek_seq;
CREATE TABLE IF NOT EXISTS public.meldingen_statistiek (
  id integer NOT NULL DEFAULT nextval('meldingen_statistiek_seq'),
  datum_melding date,
  buurtcode character varying,
  buurtnaam character varying,
  buurt character varying,
  wijkcode character varying,
  wijknaam character varying,
  wijk character varying,
  stadsdeelcode character varying,
  stadsdeelnaam character varying,
  stadsdeel character varying,
  categorie character varying,
  subrubriek character varying,
  aantal integer
);
BEGIN;
TRUNCATE public.meldingen_statistiek;
ALTER SEQUENCE meldingen_statistiek_seq RESTART WITH 1;
INSERT INTO public.meldingen_statistiek(
  datum_melding,
  buurtcode,
  buurtnaam,
  buurt,
  wijkcode,
  wijknaam,
  wijk,
  stadsdeelcode,
  stadsdeelnaam,
  stadsdeel,
  categorie,
  subrubriek,
  aantal)
SELECT melding_datum_melding,
       melding_buurt_code,
       melding_buurt_naam,
       melding_buurt_id,
       melding_wijk_code,
       melding_wijk_naam,
       melding_wijk_id,
       melding_stadsdeel_code,
       melding_stadsdeel_naam,
       melding_stadsdeel_id,
       melding_categorie,
       melding_subrubriek,
       melding_aantal
FROM pte.mora_melding_dag_buurt_categorie;
DROP TABLE pte.mora_melding_dag_buurt_categorie CASCADE;
COMMIT;
SQL


