#!/bin/bash
# usage: ./run_ingest.sh <task-id> <datahub-version> <plugins-required> <tmp-dir> <recipe_file> <report_file>

# set -euo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit

task_id="$1"
datahub_version="$2"
plugins="$3"
tmp_dir="$4"
recipe_file="$5"
report_file="$6"
debug_mode="$7"

# Execute DataHub recipe, based on the recipe id.
echo "datahub ingest run -c ${recipe_file}"
if (datahub ingest run -c "${recipe_file}"); then
  exit 0
else
  exit 1
fi
