<img alt="The Daemon, maybe" src="/img/cyberiad_1.jpg?raw=true" style="float: left; height: 300px; padding-right: 30px;">
<h3 style="margin-top: -10px; font-weight: bold">Maxwell = Mysql + Kafka</h3>

This is Maxwell's daemon, an application that reads MySQL binlogs and writes changed rows to Kafka as JSON.
It's working at the same goals as mypipe and databus, but differentiates itself with these feautures:

- Works with an unpatched mysql
- Follows all ALTER statements, which allows Maxwell to always output correct JSON
- Requires no external dependencies (save Kafka, if used)
- Minimal setup

Maxwell is intended as a source for event-based readers, eg various ETL applications, search indexing,
stat emitters.
<br style="clear:both"/>

```
mysql> insert into test.maxwell set id = 11, daemon = 'firebus!  firebus!';
(maxwell)
{"table":"maxwell","type":"insert","data":[{"id":11,"daemon":"firebus!  firebus!"}]}
```

