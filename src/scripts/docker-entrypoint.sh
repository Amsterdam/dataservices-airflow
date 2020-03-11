#!/bin/bash

set -e
set -u
set -x

echo $@
exec "$@"
