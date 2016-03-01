### Command line options
***
option                                        | description | default
--------------------------------------------- | ----------- | -------
--config FILE                                 | location of `config.properties` file |
--log_level                                   | log level [DEBUG&#124;INFO &#124;WARN&#124;ERROR | INFO
&nbsp;
--host HOST                                   | mysql host |
--user USER                                   | mysql username |
--password PASSWORD                           | mysql password | (none)
--port PORT                                   | mysql port | 3306
--schema_database                             | database name where maxwell stores schema and state | maxwell
--max_schemas                                 | how many old schemas maxwell should leave lying around in maxwell.schemas | 5
&nbsp;
--producer PRODUCER                           | what type of producer to use: [stdout, kafka, file, profiler] | stdout
--output_file                                 | if using the file producer, write JSON rows to this path |
--kafka.bootstrap.servers                     | list of kafka brokers, listed as HOST:PORT[,HOST:PORT] |
--kafka_partition_hash                        | which hash function to use: [default, murmur3] | default
--kafka_partition_by                          | what fields to hash for partition key: [database, table, primary_key] | database
--kafka_topic                                 | kafka topic to write to. | maxwell
&nbsp;
--replication_host                            | mysql host to replicate from.  Only specify if different from `host` (see notes) | schema-store host
--replication_password                        | password on replication server | (none)
--replication_port                            | port on replication server | 3306
--replication_user                            | user on replication server
&nbsp;
--include_dbs PATTERN                         | only send updates from these databases |
--exclude_dbs PATTERN                         | ignore updates from these databases |
--include_tables PATTERN                      | only send updates from tables named like PATTERN |
--exclude_tables PATTERN                      | ignore updates from tables named like PATTERN |
--blacklist_dbs PATTERN                       | ignore updates AND schema changes from databases (see warnings below)|
--blacklist_tables PATTERN                    | ignore updates AND schema changes from tables named like PATTERN (see warnings below)|
&nbsp;
--bootstrapper                                | bootstrapper type: async|sync|none. | async
--bootstrapper_fetch_size                     | number of rows fetched at a time during bootstrapping. | 64000
&nbsp;
--init_position FILE:POSITION                 | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties.
--replay                                      | enable maxwell's read-only "replay" mode.  Not available in config.properties.

### Properties file
***
If maxwell finds the file `config.properties` in $PWD it will use it.  Any
command line options (except init_position and replay) may be given as
"key=value" pairs.

Additionally, any configuration file options prefixed with 'kafka.' will be
passed into the kafka producer library, after having 'kafka.' stripped off the
front of the key.  So, for example if config.properties contains

```
kafka.batch.size=16384
```

then Maxwell will send `batch.size=16384` to the kafka producer library.

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

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>

