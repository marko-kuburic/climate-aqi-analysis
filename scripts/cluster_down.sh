#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Change to project directory
cd "$PROJECT_DIR" || { echo "Failed to change to project directory"; exit 1; }

echo "> Bringing down cluster services"

read -p "> Delete volumes? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo ">> Shutting down Airflow"
    docker-compose -f Airflow/docker-compose.yml down -v

    echo ">> Shutting down Streaming Services"
    docker-compose -f Streaming/docker-compose.yml down -v

    echo ">> Shutting down Metabase"
    docker-compose -f Metabase/docker-compose.yml down -v

    echo ">> Shutting down MongoDB"
    docker-compose -f MongoDB/docker-compose.yml down -v

    echo ">> Shutting down Apache Spark"
    docker-compose -f Spark/docker-compose.yml down -v

    echo ">> Shutting down Hadoop"
    docker-compose -f Hadoop/docker-compose.yml down -v
else
    echo ">> Shutting down Airflow"
    docker-compose -f Airflow/docker-compose.yml down

    echo ">> Shutting down Streaming Services"
    docker-compose -f Streaming/docker-compose.yml down

    echo ">> Shutting down Metabase"
    docker-compose -f Metabase/docker-compose.yml down

    echo ">> Shutting down MongoDB"
    docker-compose -f MongoDB/docker-compose.yml down

    echo ">> Shutting down Apache Spark"
    docker-compose -f Spark/docker-compose.yml down

    echo ">> Shutting down Hadoop"
    docker-compose -f Hadoop/docker-compose.yml down
fi

echo "> Deleting 'asvsp' network"
docker network rm asvsp-project