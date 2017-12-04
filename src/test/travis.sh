#!/bin/bash

set -eux
echo travis_fold:start:compile
KAFKA_VERSION=0.8.2.2 make depclean compile
KAFKA_VERSION=0.9.0.1 make depclean compile
KAFKA_VERSION=0.10.0.1 make depclean compile
KAFKA_VERSION=0.10.2.1 make depclean compile
KAFKA_VERSION=0.11.0.1 make depclean compile
make depclean
echo travis_fold:end:compile
make test
