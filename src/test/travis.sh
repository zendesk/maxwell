#!/bin/bash

MAVEN_STUPID_PATTERN="Downloading:|Downloaded:"
set -eux
echo travis_fold:start:compile
KAFKA_VERSION=0.8.2.2 make depclean compile | grep -vE "$MAVEN_STUPID_PATTERN"
KAFKA_VERSION=0.9.0.1 make depclean compile | grep -vE "$MAVEN_STUPID_PATTERN"
KAFKA_VERSION=0.10.0.1 make depclean compile | grep -vE "$MAVEN_STUPID_PATTERN"
KAFKA_VERSION=0.10.2.1 make depclean compile | grep -vE "$MAVEN_STUPID_PATTERN"
KAFKA_VERSION=0.11.0.1 make depclean compile | grep -vE "$MAVEN_STUPID_PATTERN"
make depclean 2>&1 | grep -vE "$MAVEN_STUPID_PATTERN"
echo travis_fold:end:compile
make test 2>&1 | grep -vE "$MAVEN_STUPID_PATTERN"
exit ${PIPESTATUS[0]}
