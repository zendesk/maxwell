#!/usr/bin/env bash

set -ex

mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true
bin/maxwell --kafka_version 2.7.0 --config vitess/config.properties
