#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Change to project directory
cd "$PROJECT_DIR" || { echo "Failed to change to project directory"; exit 1; }

echo "> Starting up cluster"

read -p "> Restore Metabase database? [y/N] " -n 1 -r
echo

echo "> Creating docker network 'asvsp-project'"
docker network create asvsp-project 2>/dev/null || true

echo ">> Starting up HDFS"
docker-compose -f Hadoop/docker-compose.yml up -d

echo ">> Starting up Apache Spark"
docker-compose -f Spark/docker-compose.yml up -d

echo ">> Starting up MongoDB"
docker-compose -f MongoDB/docker-compose.yml up -d

echo ">> Starting up Metabase"
docker-compose -f Metabase/docker-compose.yml up -d

echo ">> Starting up Airflow"
docker-compose -f Airflow/docker-compose.yml up -d

echo ">> Starting up Streaming Services (Kafka, Zookeeper, Producers, Consumers)"
docker-compose -f Streaming/docker-compose.yml up -d

sleep 25

echo "> Setting up services"

echo ">> Setting up Airflow objects"
docker exec airflow-airflow-apiserver-1 bash -c "/opt/airflow/config/setupObjects.sh"

if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo ">> Restoring Metabase database"
    docker exec mb-postgres bash -c "/config/restoreDB.sh"
fi