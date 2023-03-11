This is Maxwell's daemon, a [change data capture](https://www.confluent.io/blog/how-change-data-capture-works-patterns-solutions-implementation/) application 
that reads MySQL binlogs and writes data changes as JSON to Kafka, Kinesis, and other streaming platforms.



[↓ Download](https://github.com/zendesk/maxwell/releases/download/v1.39.6/maxwell-1.39.6.tar.gz) \|
[⚝ Source / Community](https://github.com/zendesk/maxwell) \|
[☝ Getting Started](/quickstart) \|
[☷ Reference](/config)

<b>What's it for?</b>

- ETL of all sorts
- maintaining an audit log of all changes to your database
- cache building/expiring
- search indexing 
- inter-service communication


<div>

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

</div>
