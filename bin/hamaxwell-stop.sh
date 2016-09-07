#!/bin/bash

base_dir=$(dirname $0)/..

PROCESSID=$base_dir/pid

cat $PROCESSID | xargs kill -9

rm $PROCESSID