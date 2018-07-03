<div id="maxwell-header">
</div>

This is Maxwell's daemon, an application that reads MySQL binlogs and writes
row updates to Kafka, Kinesis, RabbitMQ, Google Cloud Pub/Sub, or Redis (Pub/Sub or LPUSH) as JSON.  Maxwell has a
low operational bar and produces a consistent, easy to ingest stream of updates.
It allows you to easily "bolt on" some of the benefits of stream processing
systems without going through your entire code base to add (unreliable)
instrumentation points.  Common use cases include ETL, cache building/expiring,
metrics collection, search indexing and inter-service communication.

- Can do `SELECT * from table` (bootstrapping) initial loads of a table.
- supports automatic position recover on master promotion
- flexible partitioning schemes for Kakfa - by database, table, primary key, or column
- Maxwell pulls all this off by acting as a full mysql replica, including a SQL
  parser for create/alter/drop statements (nope, there was no other way).

&rarr; Download:
[https://github.com/zendesk/maxwell/releases/download/v1.17.1/maxwell-1.17.1.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.17.1/maxwell-1.17.1.tar.gz)
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
