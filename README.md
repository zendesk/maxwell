<h3 id="maxwell-header" style="margin-top: -10px; font-weight: bold">Maxwell = Mysql + Kafka</h3>


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
<br style="clear:both"/>

```
mysql> insert into test.maxwell set id = 11, daemon = 'firebus!  firebus!';

(maxwell)
{
 "database":"test",
 "table":"maxwell",
 "type":"insert",
 "data":{"id":11,"daemon":"firebus!  firebus!"}
}
```

<script>
  jQuery(document).ready(function () {
    jQuery("#maxwell-header").append(
      jQuery("<img alt='The Daemon, maybe' src='./img/cyberiad_1.jpg' style='float: left; height: 300px; padding-right: 30px;'>")

    )
  });
</script>
