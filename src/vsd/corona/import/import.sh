#!/usr/bin/env bash
set -e
source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"
FILENAME_HANDHAVING=corona_handhaving.csv
FILENAME_RIVM=corona_gevallen.json
OBJECTSTORE_PATH_HANDHAVING=corona/handhaving/$FILENAME_HANDHAVING
OBJECTSTORE_PATH_RIVM=corona/gevallen/$FILENAME_RIVM
FILENAME_HANDHAVING_PATH=${TMPDIR}/handhaving
FILENAME_RIVM_PATH=${TMPDIR}/gevallen

echo "Download source file (${FILENAME_HANDHAVING}) from oov.brievenbus.amsterdam.nl"
# To test from localhost use SSH portforward from an acceptance machine
# OOV_HOST="sftp://localhost"
OOV_HOST=${OOV_BRIEVENBUS_HOST}
OOV_PORT=${OOV_BRIEVENBUS_PORT}
OOV_USER=${OOV_BRIEVENBUS_USER}
OOV_PASSWORD=${OOV_BRIEVENBUS_PASSWORD}
curl -k ${OOV_HOST}:${OOV_PORT}\/${FILENAME_HANDHAVING} --user ${OOV_USER}:${OOV_PASSWORD} -o "${FILENAME_HANDHAVING_PATH}/${FILENAME_HANDHAVING}" --create-dirs

echo "Upload source ${FILENAME_HANDHAVING} (csv) from oov.brievenbus.amsterdam.nl to Object Store"
curl -X PUT -T ${FILENAME_HANDHAVING_PATH}/${FILENAME_HANDHAVING} --user ${OS_USERNAME}:${OS_PASSWORD} https://${OS_TENANT_NAME}.objectstore.eu/${OBJECTSTORE_PATH_HANDHAVING}

echo "Download ${FILENAME_HANDHAVING} from objectstore as the startingpoint (the staging area) for processing"
python ${SHARED_DIR}/utils/get_objectstore_file.py "${OBJECTSTORE_PATH_HANDHAVING}"

echo "Download source ${FILENAME_RIVM} (json) from RIVM gevallen"
curl -k "${AIRFLOW_CONN_RIVM_BASE_URL}/COVID-19_aantallen_gemeente_per_dag.json" -o "${FILENAME_RIVM_PATH}/${FILENAME_RIVM}" --create-dirs

echo "Upload source ${FILENAME_RIVM} (json) from RIVM gevallen to Object Store"
curl -X PUT -T ${FILENAME_RIVM_PATH}/${FILENAME_RIVM} --user ${OS_USERNAME}:${OS_PASSWORD} https://${OS_TENANT_NAME}.objectstore.eu/${OBJECTSTORE_PATH_RIVM}

echo "Download ${FILENAME_RIVM} from objectstore as the startingpoint (the staging area) for processing"
python ${SHARED_DIR}/utils/get_objectstore_file.py "${OBJECTSTORE_PATH_RIVM}"

psql -X --set ON_ERROR_STOP=on << SQL
DROP TABLE IF EXISTS corona_handhaving_new CASCADE;
DROP TABLE IF EXISTS corona_gevallen_new CASCADE;
SQL

echo "processing handhaving data"
python ${SCRIPT_DIR}/import_handhaving.py ${FILENAME_HANDHAVING_PATH}/${FILENAME_HANDHAVING}

echo "checking handhaving data"
python ${SCRIPT_DIR}/check_imported_data_handhaving.py

echo "processing rivm data"
python ${SCRIPT_DIR}/import_gevallen.py ${FILENAME_RIVM_PATH}/${FILENAME_RIVM}

echo "checking rivm gevallen data"
python ${SCRIPT_DIR}/check_imported_data_gevallen.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS corona_handhaving CASCADE;
ALTER TABLE corona_handhaving_new RENAME TO corona_handhaving;
ALTER INDEX ix_corona_handhaving_new_id RENAME TO ix_corona_handhaving_id;
DROP TABLE IF EXISTS corona_gevallen CASCADE;
ALTER TABLE corona_gevallen_new RENAME TO corona_gevallen;
ALTER INDEX ix_corona_gevallen_new_index RENAME TO ix_corona_gevallen_id;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh

