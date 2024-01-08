#!/bin/bash
# shellcheck disable=SC1091

FILENAME="$1"

if [ -f ".env" ]; then
    source .env
fi

export JDBC_USERNAME="$JDBC_USERNAME"
export JDBC_PASSWORD="$JDBC_PASSWORD"

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --driver-class-path lib/postgresql-42.5.2.jar \
    "$FILENAME"
