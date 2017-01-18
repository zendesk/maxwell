<div id="maxwell-header">
</div>

This is Maxwell's daemon, an application that reads MySQL binlogs and writes
row updates to Kafka as JSON.  Maxwell has a low operational bar and produces a
consistent, easy to ingest stream of updates.  It allows you to easily "bolt
on" some of the benefits of stream processing systems without going through your
entire code base to add (unreliable) instrumentation points.  Common use cases
include ETL, cache building/expiring, metrics collection, and search indexing.

advanced features:

- Can do `SELECT * from table` (bootstrapping) initial loads of a table
- supports automatic position recover on master promotion
- flexible partitioning schemes for Kakfa - by database, table, primary key, or column
- Maxwell pulls all this off by acting as a full mysql replica, including a SQL
  parser for create/alter/drop statements (nope, there was no other way).

&rarr; Download:
[https://github.com/zendesk/maxwell/releases/download/v1.7.0/maxwell-1.7.0.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.7.0/maxwell-1.7.0.tar.gz)
<br/>
&rarr; Source:
[https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)

<br style="clear:both"/>


```
  mysql> insert into `test`.`maxwell` set id = 1, daemon = 'Stanislaw Lem';
  maxwell: {
    "database": "test",
    "table": "maxwell",
    "type": "insert",
    "ts": 1449786310,
    "xid": 940752,
    "commit": true,
    "data": { "id":1, "daemon": "Stanislaw Lem" }
  }
```

```
  mysql> update test.maxwell set daemon = 'firebus!  firebus!' where id = 1;
  maxwell: {
    "database": "test",
    "table": "maxwell",
    "type": "update",
    "ts": 1449786341,
    "xid": 940786,
    "commit": true,
    "data": {"id":1, "daemon": "Firebus!  Firebus!"},
    "old":  {"daemon": "Stanislaw Lem"}
  }
```



<script>
  jQuery(document).ready(function () {
    jQuery("#maxwell-header").append(
      jQuery("<img alt='The Daemon, maybe' src='./img/cyberiad_1.jpg' id='maxwell-daemon-image'>")
    );
    jQuery("pre").addClass("home-code");
  });
</script>

# Docker
```
docker pull osheroff/maxwell
```

## Kafka Producer
```
docker run -it --rm osheroff/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kafka --kafka.bootstrap.servers=$KAFKA_HOST:$KAFKA_PORT
```

## STDOUT Producer
```
docker run -it --rm osheroff/maxwell bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=stdout
```

## AWS Kinesis Producer
```
docker run -it --rm --name maxwell -v `cd && pwd`/.aws:/root/.aws maxwell sh -c 'cp /app/kinesis-producer-library.properties.example /app/kinesis-producer-library.properties && echo "Region=$AWS_DEFAULT_REGION" >> /app/kinesis-producer-library.properties && bin/maxwell --user=$MYSQL_USERNAME --password=$MYSQL_PASSWORD --host=$MYSQL_HOST --producer=kinesis --kinesis_stream=$KINESIS_STREAM'
```
