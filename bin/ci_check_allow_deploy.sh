#!/usr/bin/env bash

if git log -1 --pretty=format:"%s" | grep -- --no-deploy; then
  echo not deploying due to "'--no-deploy'" text in commit message
  exit 1
fi

if git log -1 --pretty=format:"%s" | grep "automatic update of anyway-dev-commit.txt"; then
  echo not deploying because this is an automatic update of anyway dev commit
  echo it will be picked up independently by the Airflow tasks
  exit 1
fi

if git log -1 --pretty=format:"%s" | grep "automatic update of anyway-master-commit.txt"; then
  echo not deploying because this is an automatic update of anyway master commit
  echo it will be picked up independently by the Airflow tasks
  exit 1
fi

exit 0
