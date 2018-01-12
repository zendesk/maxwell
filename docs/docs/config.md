### Mysql Server configuration
***
At a minimum, Maxwell needs row-level-replication turned on into order to
operate:

```
[mysqld]
server-id=1
log-bin=master
binlog_format=row
```

### Properties file
***
If maxwell finds the file `config.properties` in $PWD it will use it.  Any
command line options (except `init_position`, `replay`, and `kafka_version`) may be given as
"key=value" pairs.

Additionally, any configuration file options prefixed with 'kafka.' will be
passed into the kafka producer library, after having 'kafka.' stripped off the
front of the key.  So, for example if config.properties contains

```
kafka.batch.size=16384
```

then Maxwell will send `batch.size=16384` to the kafka producer library.


### GTID support
***
As of 1.8.0, Maxwell contains support for
[GTID-based replication](https://dev.mysql.com/doc/refman/5.6/en/replication-gtids.html).  Enable it with the `--gtid_mode` configuration param.

Here's how you might configure your mysql server for GTID mode:

```
$ vi my.cnf

[mysqld]
server-id=1
log-bin=master
binlog_format=row
gtid-mode=ON
log-slave-updates=ON
enforce-gtid-consistency=true
```

When in GTID-mode, Maxwell will transparently pick up a new replication
position after a master change.  Note that you will still have to re-point
maxwell to the new master.

GTID support in Maxwell is considered alpha-quality at the moment; notably,
Maxwell is unable to transparently upgrade from a traditional-replication
scenario to a GTID-replication scenario; currently, when you enable gtid mode
Maxwell will recapture the schema and GTID-position from "wherever the master
is at".


### RDS configuration
***
To run Maxwell against RDS, (either Aurora or Mysql) you will need to do the following:

- set binlog_format to "ROW".  Do this in the "parameter groups" section.  For a Mysql-RDS instance this parameter will be
  in a "DB Parameter Group", for Aurora it will be in a "DB Cluster Parameter Group".
- setup RDS binlog retention as described [here](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.MySQL.html).
  The tl;dr is to execute `call mysql.rds_set_configuration('binlog retention hours', 24)` on the server.

### Filters
***
#### Include/Exclude
The options `include_dbs`, `exclude_dbs`, `include_tables`, and `exclude_tables` control whether
Maxwell will send an update for a given row to its producer.  All the options take a single value PATTERN,
which may either be a literal table/database name, given as `option=name`, or a regular expression,
given as `option=/regex/`.  The options are evaluated as follows:

1. only accept databases in `include_dbs` if non-empty
1. reject databases in `exclude_dbs`
1. only accept tables in `include_tables` if non-empty
1. reject tables in `exclude_tables`

So an example like `--include_dbs=/foo.*/ --exclude_tables=bar` will include `footy.zab` and exclude `footy.bar`

The option `blacklist_tables` and `blacklist_dbs` controls whether Maxwell will send updates for a table to its producer AND whether
it captures schema changes for that table or database. Note that once Maxwell has been running with a table or database marked as blacklisted,
you *must* continue to run Maxwell with that table or database blacklisted or else Maxwell will halt. If you want to stop
blacklisting a table or database, you will have to drop the maxwell schema first.

#### Supressing columns

If you wish to suppress columns from Maxwell's output (for instance, a password field),
you can use `exclude_columns` to filter out columns by name.

#### Filtering on column values
Maxwell can filter rows to only match when a column contains a specific value.  The `include_column_values` option takes a comma-separated
list of column/value pairs: "bar=x,foo=y".  Note that if a column does not exist in a table, it will ignore the value-filter.

### Schema storage host vs replica host
***
Maxwell needs two sets of mysql permissions to operate properly: a mysql database in which to store schema snapshots,
and a mysql host to replicate from.  The recommended configuration is that
these two functions are provided by a single mysql host.  In this case, just
specify `host`, `user`, etc.

Some configurations, however, may need to write data to a different server than it replicates from.  In this case,
the host described by `host`, `user`, ..., will be used to write schema information to, and Maxwell will replicate
events from the host described by `replication_host`, `replication_user`, ...  Note that bootstrapping is not available
in this configuration.

It's also possible to specify a different host to capture schema from than to
replicate from, via `schema_host`, ...  This is useful when using Maxscale as a
replication proxy.

### running multiple instances of maxwell against the same master
***
Maxwell can operate with multiple instances running against a single master, in
different configurations.  This can be useful if you wish to have producers
running in different configurations, for example producing different groups of
tables to different topics.  Each instance of Maxwell must be configured with a
unique `client_id`, in order to store unique binlog positions.

### multiple instances on a 5.5 server
***
With MySQL 5.5 and below, each replicator (be it mysql, maxwell, whatever) must
also be configured with a unique `replica_server_id`.  This is a 32-bit integer
that corresponds to mysql's `server_id` parameter.  The value you configure
should be unique across all mysql and maxwell instances.

### reference
***

At the minimum, you will need to specify 'host', 'user', 'password', 'producer'.
The kafka producer requires 'kafka.bootstrap.servers', the kinesis producer requires 'kinesis_stream'.

option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
**general options**
config                         | STRING                              | location of `config.properties` file                | $PWD/config.properties
log_level                      | [debug &#124; info &#124; warn &#124; error]             | log level                                           | INFO
&nbsp;
**mysql options**
host                           | STRING                              | mysql host                                          | localhost
user                           | STRING                              | mysql username                                      |
password                       | STRING                              | mysql password                                      | (no password)
port                           | INT                                 | mysql port                                          | 3306
jdbc_options                   | STRING                              | mysql jdbc connection options                       | zeroDateTimeBehavior=convertToNull&amp;connectTimeout=5000
schema_database                | STRING                              | database to store schema and position in            | maxwell
client_id                      | STRING                              | unique text identifier for maxwell instance         | maxwell
replica_server_id              | LONG                                | unique numeric identifier for this maxwell instance | 6379 (see notes)
master_recovery                | BOOLEAN                             | enable experimental master recovery code            | false
gtid_mode                      | BOOLEAN                             | enable GTID-based replication                       | false
ignore_producer_error          | BOOLEAN                             | Maxwell will be terminated on kafka/kinesis errors when false. Otherwise, those producer errors are only logged. | true
&nbsp;
replication_host               | STRING                              | mysql host to replicate from.  Only specify if different from `host` (see notes) | *schema-store host*
replication_password           | STRING                              | password on replication server | (none)
replication_port               | INT                                 | port on replication server | 3306
replication_user               | STRING                              | user on replication server |
&nbsp;
schema_host                    | STRING                              | mysql host to capture schema from.  Useful for using Maxscale as a binlog-proxy | *schema-store host*
schema_password                | STRING                              | password on schema-capture server | (none)
schema_port                    | INT                                 | port on schema-capture server | 3306
schema_user                    | STRING                              | user on schema-capture server |
&nbsp;
**producer options**
producer                       | [stdout &#124; kafka &#124; file &#124; profiler]        | type of producer to use                             | stdout
output_file                    | STRING                              | output file for `file` producer                     |
&nbsp;
kafka.bootstrap.servers        | STRING                              | kafka brokers, given as `HOST:PORT[,HOST:PORT]`     |
kafka_topic                    | STRING                              | kafka topic to write to. static string or variable replacement                            | maxwell
producer_partition_by             | [database &#124; table &#124; primary_key &#124; column] | input to kafka partition function                   | database
producer_partition_columns        | STRING                              | if partitioning by 'column', a comma separated list of columns |
producer_partition_by_fallback    | [database &#124; table &#124; primary_key]        | required when producer_partition_by=column.  Used when the column is missing |
kafka_partition_hash           | [default &#124; murmur3]                   | hash function to use when hoosing kafka partition   | default
ddl_kafka_topic                | STRING                              | if output_ddl is true, kafka topic to write DDL changes to | *kafka_topic*
kafka_version                  | [0.8.2.2 &#124; 0.9.0.1 &#124; 0.10.0.1 &#124; 0.10.2.1 &#124; 0.11.0.1]                      | run maxwell with kafka producer 0.8.2.2, 0.9.0.1, 0.10.0.1, 0.10.2.1 or 0.11.0.1  Not available in config.properties. | 0.11.0.1
&nbsp;
kinesis_stream                 | STRING                              | kinesis stream name |
&nbsp;
**formatting**
output_binlog_position         | BOOLEAN                             | should produced records include binlog position     | false
output_commit_info             | BOOLEAN                             | should produced records include commit and xid      | true
output_nulls                   | BOOLEAN                             | produced records include fields with NULL values    | true
output_server_id               | BOOLEAN                             | produced records include server_id                  | false
output_thread_id               | BOOLEAN                             | produced records include thread_id                  | false
output_ddl                     | BOOLEAN                             | output DDL (table-alter, table-create, etc) events  | false
&nbsp;
**filtering**
include_dbs                    | PATTERN                             | only send updates from these databases |
exclude_dbs                    | PATTERN                             | ignore updates from these databases |
include_tables                 | PATTERN                             | only send updates from tables named like PATTERN |
exclude_tables                 | PATTERN                             | ignore updates from tables named like PATTERN |
blacklist_dbs                  | PATTERN                             | ignore updates AND schema changes from databases (see warnings below) |
blacklist_tables               | PATTERN                             | ignore updates AND schema changes from tables named like PATTERN (see warnings below) |
&nbsp;
**encryption**
encrypt                        | [ none &#124; data &#124; all ]     | encrypt mode: none = no encryption. "data": encrypt the `data` field only. `all`: encrypt entire maxwell message | none
secret_key                     | STRING                              | specify the encryption key to be used               | null
**monitoring**
metrics_prefix | STRING | the prefix maxwell will apply to all metrics | MaxwellMetrics
metrics_type         | [slf4j &#124; jmx &#124; http &#124; datadog]      | how maxwell metrics will be reported, at least one of slf4j &#124; jmx &#124; http &#124; datadog|
metrics_slf4j_interval     | INT                                 | the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured | 60
metrics_http_port         | INT                                 | the port the server will bind to when http reporting is configured | 8080
metrics_datadog_type | [udp &#124; http] | when metrics_type includes `datadog` this is the way metrics will be reported, can only be one of [udp &#124; http] | udp
metrics_datadog_tags | STRING | datadog tags that should be supplied, e.g. tag1:value1,tag2:value2 |
metrics_datadog_interval | INT | the frequency metrics are pushed to datadog, in seconds | 60
metrics_datadog_apikey | STRING | the datadog api key to use when metrics_datadog_type = `http` |
metrics_datadog_host | STRING | the host to publish metrics to when metrics_datadog_type = `udp` | localhost
metrics_datadog_port | INT | the port to publish metrics to when metrics_datadog_type = `udp` | 8125
&nbsp;
**misc**
bootstrapper                   | [async &#124; sync &#124; none]                   | bootstrapper type.  See bootstrapping docs.        | async
init_position                  | FILE:POSITION:HEARTBEAT             | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties. |
replay                         | BOOLEAN                             | enable maxwell's read-only "replay" mode: don't store a binlog position or schema changes.  Not available in config.properties. |
