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
DROP TABLE IF EXISTS pte.mora_melding_dag_gebied_categorie CASCADE;
CREATE SCHEMA IF NOT EXISTS pte;
\i ${TMPDIR}/mora.backup
SQL

psql -X --set ON_ERROR_STOP=on << SQL
CREATE SEQUENCE IF NOT EXISTS public.meldingen_statistieken_seq;
CREATE TABLE IF NOT EXISTS public.meldingen_statistieken (
  id integer NOT NULL DEFAULT nextval('meldingen_statistieken_seq'),
  gebiedstype character varying,
  datum_melding date,
  buurtcode character varying,
  buurtnaam character varying,
  buurt_id character varying,
  wijkcode character varying,
  wijknaam character varying,
  wijk_id character varying,
  stadsdeelcode character varying,
  stadsdeelnaam character varying,
  stadsdeel_id character varying,
  gemeentecode character varying,
  gemeentenaam character varying,
  categorie character varying,
  subrubriek character varying,
  aantal integer
);
BEGIN;
TRUNCATE public.meldingen_statistieken;
ALTER SEQUENCE meldingen_statistieken_seq RESTART WITH 1;
INSERT INTO public.meldingen_statistieken(
  gebiedstype,
  datum_melding,
  buurtcode,
  buurtnaam,
  buurt_id,
  wijkcode,
  wijknaam,
  wijk_id,
  stadsdeelcode,
  stadsdeelnaam,
  stadsdeel_id,
  gemeentecode,
  gemeentenaam,
  categorie,
  subrubriek,
  aantal)
SELECT melding_gebiedstype,
       melding_datum_melding,
       melding_buurt_code,
       melding_buurt_naam,
       melding_buurt_id,
       melding_wijk_code,
       melding_wijk_naam,
       melding_wijk_id,
       melding_stadsdeel_code,
       melding_stadsdeel_naam,
       melding_stadsdeel_id,
       melding_gemeente_code,
       melding_gemeente_naam,
       melding_categorie,
       melding_subrubriek,
       melding_aantal
FROM pte.mora_melding_dag_gebied_categorie;
DROP TABLE pte.mora_melding_dag_gebied_categorie CASCADE;
COMMIT;
SQL


