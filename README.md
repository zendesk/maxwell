# Maxwell's daemon

This is Maxwell's daemon, an application that processes MySQL binlogs and outputs the changesets as JSON to Kafka.
It's conceptually similar to databus and mypipe, but it can:

- follow schema changes coming down the replication stream
- output the changed rows as JSON (should we support avro?  I dunno.)
- recover the binlog position where it left off.

It's intended as an ETL tool and as a source for event-based services.

## quickstart

this will start a Maxwell's daemon for you to play around with:

```
mysql> GRANT ALL on maxwell.* to 'maxwell'@'%' identified by 'XXXXXX';
mysql> GRANT SELECT on *.* to 'maxwell'@'%';
mysql> GRANT REPLICATION CLIENT ON *.* TO 'maxwell'@'%;

curl -sLo - https://github.com/zendesk/maxwell/releases/download/v0.1/maxwell-0.1.0.tar.gz  | tar zxvf -
cd maxwell-0.1.0
bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --producer=stdout

```

## something actually useful

```
cp config.properties.example config.properties
# edit config.properties
bin/maxwell
```

