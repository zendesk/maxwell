### Command line options
***
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
**general options**
config                         | STRING                              | location of `config.properties` file                |
log_level                      | [debug &#124; info &#124; warn &#124; error]             | log level                                           | INFO
&nbsp;
**mysql options**
host                           | STRING                              | mysql host                                          | localhost
user                           | STRING                              | mysql username                                      |
password                       | STRING                              | mysql password                                      | (no password)
port                           | INT                                 | mysql port                                          | 3306
schema_database                | STRING                              | database to store schema and position in            | maxwell
client_id                      | STRING                              | unique text identifier for maxwell instance         | maxwell
replica_server_id              | LONG                                | unique numeric identifier for this maxwell instance | 6379 (see notes)
master_recovery                | BOOLEAN                             | enable experimental master recovery code            | false
&nbsp;
replication_host               | STRING                              | mysql host to replicate from.  Only specify if different from `host` (see notes) | *schema-store host*
replication_password           | STRING                              | password on replication server | (none)
replication_port               | INT                                 | port on replication server | 3306
replication_user               | STRING                              | user on replication server |
&nbsp;
**producer options**
producer                       | [stdout &#124; kafka &#124; file &#124; profiler]        | type of producer to use                             | stdout
output_file                    | STRING                              | output file for `file` producer                     |
&nbsp;
kafka.bootstrap.servers        | STRING                              | kafka brokers, given as `HOST:PORT[,HOST:PORT]`     |
kafka_topic                    | STRING                              | kafka topic to write to. static string or variable replacement                            | maxwell
kafka_partition_by             | [database &#124; table &#124; primary_key &#124; column] | input to kafka partition function                   | database
kafka_partition_columns        | STRING                              | if partitioning by 'column', a comma separated list of columns |
kafka_partition_by_fallback    | [database &#124; table &#124; primary_key]        | required when kafka_partition_by=column.  Used when the column is missing |
kafka_partition_hash           | [default &#124; murmur3]                   | hash function to use when hoosing kafka partition   | default
ddl_kafka_topic                | STRING                              | if output_ddl is true, kafka topic to write DDL changes to | *kafka_topic*
kafka_version                  | [0.8 &#124; 0.9 &#124; 0.10 &#124; 0.10.1]                      | run maxwell with kafka producer 0.8.2, 0.9.0, 0.10.0.1 or 0.10.1.0.  Not available in config.properties. | 0.9.0
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
**misc**
bootstrapper                   | [async &#124; sync &#124; none]                   | bootstrapper type.  See bootstrapping docs.        | async
&nbsp;
init_position                  | FILE:POSITION                       | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties. |
replay                         | BOOLEAN                             | enable maxwell's read-only "replay" mode: don't store a binlog position or schema changes.  Not available in config.properties. |

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

### Running against RDS
***
To run Maxwell against RDS, (either Aurora or Mysql) you will need to do the following:

- set binlog_format to "ROW".  Do this in the "parameter groups" section.  For a Mysql-RDS instance this parameter will be
  in a "DB Parameter Group", for Aurora it will be in a "DB Cluster Parameter Group".
- setup RDS binlog retention as described [here](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.MySQL.html).
  The tl;dr is to execute `call mysql.rds_set_configuration('binlog retention hours', 24)` on the server.


### Filters
***
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

### running multiple instances of maxwell against the same master
***
Maxwell can operate with multiple instances running against a single master, in
different configurations.  This can be useful if you wish to have producers
running in different configurations, for example producing different groups of
tables to different topics.  Each instance of Maxwell must be configured with a
unique `client_id`, in order to store unique binlog positions.

#### multiple instances on a 5.5 server

With MySQL 5.5 and below, each replicator (be it mysql, maxwell, whatever) must
also be configured with a unique `replica_server_id`.  This is a 32-bit integer
that corresponds to mysql's `server_id` parameter.  The value you configure
should be unique across all mysql and maxwell instances.

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>

