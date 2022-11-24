#!/usr/bin/env bash

set -x

DOCKER_BIN=$(which docker)
if [[ "$DOCKER_BIN" != "" ]]; then
  	echo "Docker is installed"
	COMPOSE_COMMAND="docker compose"
else
	echo "Docker is not installed, trying podman..."
	PODMAN_BIN=$(which podman)
	if [ -z "$PODMAN_BIN" ]; then
		echo "Podman is not installed either, exiting..."
		exit 1
	fi
	COMPOSE_COMMAND="podman-compose"
fi

# Stop and remove any existing containers
$COMPOSE_COMMAND -f vitess/docker-compose.yml down
