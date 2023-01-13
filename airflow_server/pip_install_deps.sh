#!/usr/bin/env bash

ANYWAY_ETL_BRANCH="${ANYWAY_ETL_BRANCH:-main}"
ANYWAY_ETL_USE_LATEST_TAG="${ANYWAY_ETL_USE_LATEST_TAG:-no}"

if [ "${ANYWAY_ETL_USE_LATEST_TAG}" == "yes" ]; then
  NEW_ANYWAY_ETL_COMMIT="$(curl -fs https://api.github.com/repos/data-for-change/anyway-etl/releases/latest | jq -r .tag_name)"
  ANYWAY_COMMIT_URL="https://raw.githubusercontent.com/data-for-change/anyway-etl/${NEW_ANYWAY_ETL_COMMIT}/anyway-${ANYWAY_BRANCH:-dev}-commit.txt"
  ANYWAY_ETL_REQUIREMENTS_URL="https://raw.githubusercontent.com/data-for-change/anyway-etl/${NEW_ANYWAY_ETL_COMMIT}/requirements.txt"
  ANYWAY_ETL_INSTALL_URL="git+https://github.com/data-for-change/anyway-etl@${NEW_ANYWAY_ETL_COMMIT}#egg=anyway-etl"
else
  NEW_ANYWAY_ETL_COMMIT="$(curl -fs https://api.github.com/repos/data-for-change/anyway-etl/branches/${ANYWAY_ETL_BRANCH} | jq -r .commit.sha)"
  ANYWAY_COMMIT_URL="https://raw.githubusercontent.com/data-for-change/anyway-etl/${ANYWAY_ETL_BRANCH}/anyway-${ANYWAY_BRANCH:-dev}-commit.txt"
  ANYWAY_ETL_REQUIREMENTS_URL="https://raw.githubusercontent.com/data-for-change/anyway-etl/${ANYWAY_ETL_BRANCH}/requirements.txt"
  ANYWAY_ETL_INSTALL_URL="git+https://github.com/data-for-change/anyway-etl@${ANYWAY_ETL_BRANCH}#egg=anyway-etl"
fi
if expr length "${NEW_ANYWAY_ETL_COMMIT}" '>' 5; then
  HAS_NEW_ANYWAY_ETL_COMMIT="yes"
else
  HAS_NEW_ANYWAY_ETL_COMMIT="no"
fi
if [ -e "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt" ]; then
  OLD_ANYWAY_ETL_COMMIT="$(cat "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt")"
  if expr length "${OLD_ANYWAY_ETL_COMMIT}" '>' 5; then
    HAS_OLD_ANYWAY_ETL_COMMIT="yes"
  else
    HAS_OLD_ANYWAY_ETL_COMMIT="no"
  fi
else
  HAS_OLD_ANYWAY_ETL_COMMIT="no"
fi
if [ "${HAS_OLD_ANYWAY_ETL_COMMIT}" == "no" ] || [ "${HAS_NEW_ANYWAY_ETL_COMMIT}" == "no" ] || [ "${OLD_ANYWAY_ETL_COMMIT}" != "${NEW_ANYWAY_ETL_COMMIT}" ]; then
  echo Updating anyway-etl dependencies to ${NEW_ANYWAY_ETL_COMMIT}... &&\
  ANYWAY_COMMIT="$(curl -s "${ANYWAY_COMMIT_URL}")" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqr "${ANYWAY_ETL_REQUIREMENTS_URL}" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqe "git+https://github.com/data-for-change/anyway@${ANYWAY_COMMIT}#egg=anyway" &&\
  "${ANYWAY_ETL_VENV}/bin/pip" install --upgrade -qqe "${ANYWAY_ETL_INSTALL_URL}" &&\
  if [ "${HAS_NEW_ANYWAY_ETL_COMMIT}" == "yes" ]; then
    echo "${NEW_ANYWAY_ETL_COMMIT}" > "${ANYWAY_ETL_VENV}/anyway_etl_commit.txt"
  fi &&\
  echo OK
fi
