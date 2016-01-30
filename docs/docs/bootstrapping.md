*Maxwell's bootstrapping is available in the [1.0.0-PRE2 release](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-PRE2)*

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

### Using the maxwell.bootstrap table
***
Alternatively you can insert a row in the `maxwell.bootstrap` table to trigger a bootstrap.

```
mysql> insert into maxwell.bootstrap (database_name, table_name) values ('fooDB', 'barTable');
```

### Async vs Sync bootstrapping
***
The Maxwell replicator is single threaded; events are captured by one thread from the binlog and replicated to Kafka one message at a time.
When running Maxwell with `--bootstrapper=sync`, the same thread is used to do bootstrapping, meaning that all binlog events are blocked until bootstrapping is complete.
Running Maxwell with `--bootstrapper=async` however, will make Maxwell spawn a separate thread for bootstrapping.
In this async mode, non-bootstrapped tables are replicated as normal by the main thread, while the binlog events for bootstrapped tables are queued and sent to the replication stream at the end of the bootstrap process.

### Bootstrapping Data Format
***

* a bootstrap starts with a document with `type = "bootstrap-start"`
* then documents with `type = "insert"` (one per row in the table)
* then one document per `INSERT`, `UPDATE` or `DELETE` that occurred since the beginning of bootstrap
* finally a document with `type = "bootstrap-complete"`

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

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
