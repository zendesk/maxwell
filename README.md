<div id="maxwell-header">
</div>

This is Maxwell's daemon, an application that reads MySQL binlogs and writes
row updates as JSON to Kafka, Kinesis, or other streaming platforms.  Maxwell has
low operational overhead, requiring nothing but mysql and a place to write to.
Its common use cases include ETL, cache building/expiring, metrics collection,
search indexing and inter-service communication.  Maxwell gives you some of the
benefits of event sourcing without having to re-architect your entire platform.

<b>Download:</b><br>
[https://github.com/zendesk/maxwell/releases/download/v1.39.2/maxwell-1.39.2.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.39.2/maxwell-1.39.2.tar.gz)
<br/>
<b>Source:</b><br>
[https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)
<br clear="all">


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
