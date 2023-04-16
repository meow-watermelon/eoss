#!/bin/bash

set -o pipefail
set -e

function banner() {
    echo "################################"
    echo "# ERIC'S OBJECT STORAGE SYSTEM #"
    echo "################################"
    echo -e "\n"
}

banner

# check if all required binaries exist
declare -a EOSS_BINARIES
EOSS_BINARIES=("pre-start.py" "eoss.py")

for file in "${EOSS_BINARIES[@]}"
do
    if [[ ! -e "$file" ]]
    then
        echo "ERROR: $file is missing." 1>&2
        exit 2
    fi
done

# trigger pre-start.py
echo -e "##### Trigger Pre-Start Procedure #####\n"
./pre-start.py
if [[ $? -ne 0 ]]
then
    echo "ERROR: pre-start is failed." 1>&2
    exit 3
fi

# start EOSS service
echo "##### Trigger Pre-Start Procedure #####\n"
exec uwsgi ../config/eoss-uwsgi.ini
