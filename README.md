This is __Maxwell's daemon__, a [change data capture](https://www.confluent.io/blog/how-change-data-capture-works-patterns-solutions-implementation/) application 
that reads MySQL binlogs and writes data changes as JSON to Kafka, Kinesis, and other streaming platforms.



[↓ Download](https://github.com/zendesk/maxwell/releases/download/v1.41.2/maxwell-1.41.2.tar.gz) \|
[⚝ Source / Community](https://github.com/zendesk/maxwell) \|
[☝ Getting Started](/quickstart) \|
[☷ Reference](/config)

__What's it for?__

- ETL of all sorts
- maintaining an audit log of all changes to your database
- cache building/expiring
- search indexing 
- inter-service communication


__It goes like this:__

```
  mysql> update `test`.`maxwell` set mycol = 55, daemon = 'Stanislaw Lem';
  maxwell -> kafka: 
  {
    "database": "test",
    "table": "maxwell",
    "type": "update",
    "ts": 1449786310,
    "data": { "id":1, "daemon": "Stanislaw Lem", "mycol": 55 },
    "old": { "mycol":, 23, "daemon": "what once was" }
  }
```
