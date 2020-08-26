#!/usr/bin/env bash
set -e
source ${SHARED_DIR}/import/config.sh
source ${SHARED_DIR}/import/before.sh

echo "Process import data"
FILENAME=corona_handhaving.csv
OBJECTSTORE_PATH=handhavingen/$FILENAME

export AIRFLOW_CONN_SWIFT_DEFAULT=s3://${OS_USERNAME}:${OS_PASSWORD}@${OS_TENANT_NAME}
echo "Download file from objectstore"
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

