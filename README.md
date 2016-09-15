<div id="maxwell-header">
  <h2>Maxwell = Mysql + Kafka</h2>
</div>

This is Maxwell's daemon, an application that reads MySQL binlogs and writes row updates to Kafka as JSON.
It's playing in the same space as [mypipe](https://github.com/mardambey/mypipe) and [databus](http://data.linkedin.com/projects/databus),
but differentiates itself with these features:

- Works with an unpatched mysql
- Parses ALTER/CREATE/DROP table statements, which allows Maxwell to always have a correct view of the mysql schema
- Stores its replication position and needed data within the mysql server itself
- Requires no external dependencies (save Kafka, if used)
- Eschews the complexity of Avro for plain old JSON.
- Minimal setup

Maxwell is intended as a source for event-based readers, eg various ETL applications, search indexing,
stat emitters.

- Download: [https://github.com/zendesk/maxwell/releases/download/v1.2.1/maxwell-1.2.1.tar.gz](https://github.com/zendesk/maxwell/releases/download/v1.2.1/maxwell-1.2.1.tar.gz)
- Source: [https://github.com/zendesk/maxwell](https://github.com/zendesk/maxwell)

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
