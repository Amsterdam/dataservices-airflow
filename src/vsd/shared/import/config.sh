#!/usr/bin/env bash

set -e # Exit immediately if a command exits with a non-zero status
set -u # Treat unset variables as an error when substituting

export DATASET="$(basename `cd ${SCRIPT_DIR}/..; pwd`)"
export DATA_DIR="${SCRIPT_DIR}/../data"

export CUR_DIR=`pwd`
export TMPDIR=/tmp/${DATASET}

# export PGHOST=${DATABASE_HOST_OVERRIDE:-${DATABASE_HOST:-localhost}}
# export PGPORT=${DATABASE_PORT_OVERRIDE:-${DATABASE_PORT:-5408}}
# export PGDATABASE=${DATABASE_NAME:-various_small_datasets}
# export PGUSER=${DATABASE_USER:-various_small_datasets}
# export PGPASSWORD=${DATABASE_PASSWORD:-insecure}

echo "Config ${DATASET}"
