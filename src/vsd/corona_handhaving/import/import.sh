#!/usr/bin/env bash
set -e
source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"
FILENAME=corona_handhaving.csv
OBJECTSTORE_PATH=handhavingen/$FILENAME

echo "Download source file (csv) from oov.brievenbus.amsterdam.nl"
# To test from localhost use SSH portforward from an acceptance machine
# OOV_HOST="sftp://localhost"
OOV_HOST=${OOV_BRIEVENBUS_HOST}
OOV_PORT=${OOV_BRIEVENBUS_PORT}
OOV_USER=${OOV_BRIEVENBUS_USER}
OOV_PASSWORD=${OOV_BRIEVENBUS_PASSWORD}
curl -k ${OOV_HOST}:${OOV_PORT}\/${FILENAME} --user ${OOV_USER}:${OOV_PASSWORD} -o "/tmp/corona_handhaving\/${FILENAME}" --create-dirs

echo "Upload source file (csv) from oov.brievenbus.amsterdam.nl to Object Store"
# URLdecode is needed for the password to pass
alias urldecode='python3 -c "import sys, urllib.parse as ul; print(ul.unquote_plus(sys.argv[1]))"'
OS_PASS_DECODE=$(urldecode ${OS_PASSWORD})
curl -X PUT -T /tmp/corona_handhaving/${FILENAME} --user ${OS_USERNAME}:${OS_PASS_DECODE} https://${OS_TENANT_NAME}.objectstore.eu/handhavingen/${FILENAME}

echo "Download file from objectstore as the startingpoint (the staging area) for processing"
python $SHARED_DIR/utils/get_objectstore_file.py "$OBJECTSTORE_PATH"

psql -X --set ON_ERROR_STOP=on << SQL
DROP TABLE IF EXISTS corona_handhaving_new CASCADE;
SQL

python ${SCRIPT_DIR}/import.py ${TMPDIR}/$FILENAME

python ${SCRIPT_DIR}/check_imported_data.py

echo "Rename tables"
psql -X --set ON_ERROR_STOP=on <<SQL
BEGIN;
DROP TABLE IF EXISTS corona_handhaving CASCADE;
ALTER TABLE corona_handhaving_new RENAME TO corona_handhaving;
ALTER INDEX ix_corona_handhaving_new_id RENAME TO ix_corona_handhaving_id;
COMMIT;
SQL

source ${SHARED_DIR}/import/after.sh

