### Using the maxwell-bootstrap utility
***
You can use the `maxwell-bootstrap` utility to bootstrap tables from the command-line.

option                                        | description
--------------------------------------------- | -----------
--log_level LOG_LEVEL                         | log level (DEBUG, INFO, WARN or ERROR)
--user USER                                   | mysql username
--password PASSWORD                           | mysql password
--host HOST                                   | mysql host
--port PORT                                   | mysql port
--database DATABASE                           | mysql database containing the table to bootstrap
--table TABLE                                 | mysql table to bootstrap
--where WHERE_CLAUSE                          | where clause to restrict the rows bootstrapped from the specified table
--client_id CLIENT_ID                         | specify which maxwell instance should perform the bootstrap operation

### Using the maxwell.bootstrap table
***
Alternatively you can insert a row in the `maxwell.bootstrap` table to trigger a bootstrap.

```
mysql> insert into maxwell.bootstrap (database_name, table_name) values ('fooDB', 'barTable');
```
Optionally, you can include a where clause to replay part of the data.

bin/maxwell-bootstrap --config localhost.properties --database foobar --table test --log_level info

or

bin/maxwell-bootstrap --config localhost.properties --database foobar --table test --where "my_date >= '2017-01-07 00:00:00'" --log_level info

### Async vs Sync bootstrapping
***
The Maxwell replicator is single threaded; events are captured by one thread from the binlog and replicated to Kafka one message at a time.
When running Maxwell with `--bootstrapper=sync`, the same thread is used to do bootstrapping, meaning that all binlog events are blocked until bootstrapping is complete.
Running Maxwell with `--bootstrapper=async` however, will make Maxwell spawn a separate thread for bootstrapping.
In this async mode, non-bootstrapped tables are replicated as normal by the main thread, while the binlog events for bootstrapped tables are queued and sent to the replication stream at the end of the bootstrap process.

### Bootstrapping Data Format
***

* a bootstrap starts with an event of `type = "bootstrap-start"`
* then events with `type = "bootstrap-insert"` (one per row in the table)
* then one event per `INSERT`, `UPDATE` or `DELETE` with standard event types i.e. `type = "insert"`, `type = "update"` or `type = "delete"` that occurred since the beginning of bootstrap
* finally an event with `type = "bootstrap-complete"`

Here's a complete example:
```
mysql> create table fooDB.barTable(txt varchar(255));
mysql> insert into fooDB.barTable (txt) values ("hello"), ("bootstrap!");
mysql> insert into maxwell.bootstrap (database_name, table_name) values ("fooDB", "barTable");
```
Corresponding replication stream output of table `fooDB.barTable`:
```
{"database":"fooDB","table":"barTable","type":"insert","ts":1450557598,"xid":13,"data":{"txt":"hello"}}
{"database":"fooDB","table":"barTable","type":"insert","ts":1450557598,"xid":13,"data":{"txt":"bootstrap!"}}
{"database":"fooDB","table":"barTable","type":"bootstrap-start","ts":1450557744,"data":{}}
{"database":"fooDB","table":"barTable","type":"bootstrap-insert","ts":1450557744,"data":{"txt":"hello"}}
{"database":"fooDB","table":"barTable","type":"bootstrap-insert","ts":1450557744,"data":{"txt":"bootstrap!"}}
{"database":"fooDB","table":"barTable","type":"bootstrap-complete","ts":1450557744,"data":{}}
```

### Failure Scenarios
***
If Maxwell crashes during bootstrapping the next time it runs it will rerun the bootstrap in its entirety - regardless of previous progress.
If this behavior is not desired, manual updates to the `bootstrap` table are required.
Specifically, marking the unfinished bootstrap row as 'complete' (`is_complete` = 1) or deleting the row.

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
