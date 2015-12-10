<div class="content-title">Bootstrapping</div>

***

### Using the maxwell-bootstrap utility

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

Alternatively you can insert a row in the `maxwell-bootstrap` table to trigger a bootstrap.

```
mysql> insert into maxwell.bootstrap (database_name, table_name) values ('fooDB', 'barTable');
```

### Bootstrapping Data Format

* a bootstrap starts with a document with `type = "bootstrap-start"`
* then documents with `type = "insert"` (one per row in the table)
* then one document per `INSERT`, `UPDATE` or `DELETE` that occurred since the beginning of bootstrap
* finally a document with `type = "bootstrap-complete"`

Here's a complete example:
```
mysql> create table barTable(txt varchar(255));
mysql> insert into barTable (txt) values ("hello"), ("bootstrapping!");
mysql> insert into maxwell.bootstrap (database_name, table_name) values ("fooDb", "barTable");
```
And here's the corresponding replication stream output of table `fooDB`:
```
{...,"type":"insert","ts":1450557598,"xid":13561,"data":{"txt":"hello"}}
{...,"type":"insert","ts":1450557598,"xid":13561,"data":{"txt":"bootstrapping!"}}
{...,"type":"bootstrap-start","ts":1450557744340,"data":{}}
{...,"type":"insert","ts":1450557744355,"data":{"txt":"hello"}}
{...,"type":"insert","ts":1450557744356,"data":{"txt":"bootstrapping!"}}
{...,"type":"bootstrap-complete","ts":1450557744362,"data":{}}
```

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
