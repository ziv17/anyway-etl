#!/usr/bin/env bash

NEW_ANYWAY_ETL_COMMIT="$(curl -fs https://api.github.com/repos/hasadna/anyway-etl/branches/main | jq -r .commit.sha)"
if expr length "${NEW_ANYWAY_ETL_COMMIT}" '>' 10; then
  HAS_NEW_ANYWAY_ETL_COMMIT="yes"
else
  HAS_NEW_ANYWAY_ETL_COMMIT="no"
fi
if [ -e "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt" ]; then
  OLD_ANYWAY_ETL_COMMIT="$(cat "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt")"
  if expr length "${OLD_ANYWAY_ETL_COMMIT}" '>' 10; then
    HAS_OLD_ANYWAY_ETL_COMMIT="yes"
  else
    HAS_OLD_ANYWAY_ETL_COMMIT="no"
  fi
else
  HAS_OLD_ANYWAY_ETL_COMMIT="no"
fi
if [ "${HAS_OLD_ANYWAY_ETL_COMMIT}" == "no" ] || [ "${HAS_NEW_ANYWAY_ETL_COMMIT}" == "no" ] || [ "${OLD_ANYWAY_ETL_COMMIT}" != "${NEW_ANYWAY_ETL_COMMIT}" ]; then
  echo Updating anyway-etl dependencies... &&\
  ANYWAY_COMMIT="$(curl -s "https://raw.githubusercontent.com/hasadna/anyway-etl/main/anyway-${ANYWAY_BRANCH:-dev}-commit.txt")" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqr "https://raw.githubusercontent.com/hasadna/anyway-etl/main/requirements.txt" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqe "git+https://github.com/hasadna/anyway@${ANYWAY_COMMIT}#egg=anyway" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqe "git+https://github.com/hasadna/anyway-etl@main#egg=anyway-etl" &&\
  if [ "${HAS_NEW_ANYWAY_ETL_COMMIT}" == "yes" ]; then
    echo "${NEW_ANYWAY_ETL_COMMIT}" > "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt"
  fi &&\
  echo OK
fi
