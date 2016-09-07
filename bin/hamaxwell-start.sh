#!/bin/bash

base_dir=$(dirname $0)/..

conf_file=$base_dir/config.properties
ha_conf_file=$base_dir/ha.config.properties
maxwell_log_file=$base_dir/maxwell.log

PROCESSID=$base_dir/pid

nohup $base_dir/bin/maxwell-active-standby --config=$conf_file --haconfig=$ha_conf_file $@ > $maxwell_log_file 2>&1 < /dev/null & echo $! > $PROCESSID
