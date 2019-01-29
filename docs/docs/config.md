### Reference
***

At the minimum, you will need to specify 'host', 'user', 'password', 'producer'.
The kafka producer requires 'kafka.bootstrap.servers', the kinesis producer requires 'kinesis_stream'.

option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
**general options**
config                         | STRING                              | location of `config.properties` file                | $PWD/config.properties
log_level                      | [debug &#124; info &#124; warn &#124; error]             | log level                                           | info
daemon                         |                                     | running maxwell as a daemon                         |
env_config_prefix              | STRING                              | env vars matching prefix are treated as config values |
&nbsp;
**mysql options**
host                           | STRING               | mysql host                                          | localhost
user                           | STRING               | mysql username                                      |
password                       | STRING               | mysql password                                      | (no password)
port                           | INT                  | mysql port                                          | 3306
jdbc_options                   | STRING               | mysql jdbc connection options                       | [DEFAULT_JDBC_OPTS](#jdbcopts)
ssl                            | [SSL_OPT](#sslopt)   | SSL behavior for mysql cx                           | DISABLED
schema_database                | STRING               | database to store schema and position in            | maxwell
client_id                      | STRING               | unique text identifier for maxwell instance         | maxwell
replica_server_id              | LONG                 | unique numeric identifier for this maxwell instance | 6379 (see [notes](#multiple-maxwell-instances))
master_recovery                | BOOLEAN              | enable experimental master recovery code            | false
gtid_mode                      | BOOLEAN              | enable GTID-based replication                       | false
recapture_schema               | BOOLEAN              | recapture the latest schema. Not available in config.properties. | false
&nbsp;
replication_host               | STRING               | server to replicate from.  See [split server roles](#split-server-roles) | *schema-store host*
replication_password           | STRING               | password on replication server                      | (none)
replication_port               | INT                  | port on replication server                          | 3306
replication_user               | STRING               | user on replication server                          |
replication_ssl                | [SSL_OPT](#sslopt)   | SSL behavior for replication cx cx                  | DISABLED
&nbsp;
schema_host                    | STRING               | server to capture schema from.  See [split server roles](#split-server-roles) | *schema-store host*
schema_password                | STRING               | password on schema-capture server                   | (none)
schema_port                    | INT                  | port on schema-capture server                       | 3306
schema_user                    | STRING               | user on schema-capture server                       |
schema_ssl                     | [SSL_OPT](#sslopt)   | SSL behavior for schema-capture server              | DISABLED
&nbsp;
**producer options**
producer                       | [PRODUCER_TYPE](#producer_type)     | type of producer to use                             | stdout
custom_producer.factory        | CLASS_NAME                          | fully qualified custom producer factory class, see [example](https://github.com/zendesk/maxwell/blob/master/src/example/com/zendesk/maxwell/example/producerfactory/CustomProducerFactory.java) |
producer_ack_timeout           | [PRODUCER_ACK_TIMEOUT](#ack_timeout) | time in milliseconds before async producers consider a message lost |
producer_partition_by          | [PARTITION_BY](#partition_by)       | input to kafka/kinesis partition function           | database
producer_partition_columns     | STRING                              | if partitioning by 'column', a comma separated list of columns |
producer_partition_by_fallback | [PARTITION_BY_FALLBACK](#partition_by_fallback) | required when producer_partition_by=column.  Used when the column is missing |
ignore_producer_error          | BOOLEAN              | When false, Maxwell will terminate on kafka/kinesis publish errors (aside from RecordTooLargeException). When true, errors are only logged. See also dead_letter_topic | true
&nbsp;
**"file" producer options**
output_file                    | STRING                              | output file for `file` producer                     |
javascript                     | STRING                              | file containing javascript filters |
&nbsp;
**"kafka" producer options **
kafka.bootstrap.servers        | STRING                              | kafka brokers, given as `HOST:PORT[,HOST:PORT]`     |
kafka_topic                    | STRING                              | kafka topic to write to.                            | maxwell
dead_letter_topic              | STRING                              | the topic to write a "skeleton row" (a row where `data` includes only primary key columns) when there's an error publishing a row. When `ignore_producer_error` is `false`, only RecordTooLargeException causes a fallback record to be published, since other errors cause termination. Currently only supported in Kafka publisher |
kafka_version                  | [KAFKA_VERSION](#kafka_version)     | run maxwell with specified kafka producer version.  Not available in config.properties. | 0.11.0.1
kafka_partition_hash           | [ default &#124; murmur3 ]          | hash function to use when choosing kafka partition   | default
kafka_key_format               | [ array &#124; hash ]               | how maxwell outputs kafka keys, either a hash or an array of hashes | hash
ddl_kafka_topic                | STRING                              | if output_ddl is true, kafka topic to write DDL changes to | *kafka_topic*
&nbsp;
**"kinesis" producer options **
kinesis_stream                 | STRING                              | kinesis stream name |
&nbsp;
**"sqs" producer options **
sqs_queue_uri                  | STRING                              | SQS Queue URI |
&nbsp;
**"pubsub" producer options **
pubsub_topic                   | STRING     | Google Cloud pub-sub topic |
pubsub_platform_id             | STRING     | Google Cloud platform id associated with topic |
ddl_pubsub_topic               | STRING     | Google Cloud pub-sub topic to send DDL events to |
&nbsp;
**"rabbitmq" producer options **
rabbitmq_user                  | STRING     | Username of Rabbitmq connection | guest
rabbitmq_pass                  | STRING     | Password of Rabbitmq connection | guest
rabbitmq_host                  | STRING     | Host of Rabbitmq machine
rabbitmq_port                  | INT        | Port of Rabbitmq machine |
rabbitmq_virtual_host          | STRING     | Virtual Host of Rabbitmq |
rabbitmq_exchange              | STRING     | Name of exchange for rabbitmq publisher |
rabbitmq_exchange_type         | STRING     | Exchange type for rabbitmq |
rabbitmq_exchange_durable      | BOOLEAN    | Exchange durability. | false
rabbitmq_exchange_autodelete   | BOOLEAN    | If set, the exchange is deleted when all queues have finished using it. | false
rabbitmq_routing_key_template  | STRING     | A string template for the routing key, `%db%` and `%table%` will be substituted. | `%db%.%table%`.
rabbitmq_message_persistent    | BOOLEAN    | Eanble message persistence. | false
rabbitmq_declare_exchange      | BOOLEAN    | Should declare the exchange for rabbitmq publisher | true
&nbsp;
**"redis" producer options **
redis_host                     | STRING                   | Host of Redis server | localhost
redis_port                     | INT                      | Port of Redis server | 6379
redis_auth                     | STRING                   | Authentication key for a password-protected Redis server
redis_database                 | INT                      | Database of Redis server | 0
redis_pub_channel              | STRING                   | Redis Pub/Sub channel | maxwell
redis_list_key                 | STRING                   | Redis LPUSH List Key | maxwell
redis_type                     | [ pubsub &#124; lpush ]  | Selects either Redis Pub/Sub or LPUSH. | pubsub
&nbsp;
**formatting**
output_binlog_position         | BOOLEAN  | records include binlog position     | false
output_gtid_position           | BOOLEAN  | records include gtid position, if available | false
output_commit_info             | BOOLEAN  | records include commit and xid      | true
output_xoffset                 | BOOLEAN  | records include virtual tx-row offset | false
output_nulls                   | BOOLEAN  | records include fields with NULL values    | true
output_server_id               | BOOLEAN  | records include server_id                  | false
output_thread_id               | BOOLEAN  | records include thread_id                  | false
output_schema_id               | BOOLEAN  | records include schema_id, schema_id is the id of the latest schema tracked by maxwell and doesn't relate to any mysql tracked value                  | false
output_row_query               | BOOLEAN  | records include INSERT/UPDATE/DELETE statement. Mysql option "binlog_rows_query_log_events" must be enabled | false
output_ddl                     | BOOLEAN  | output DDL (table-alter, table-create, etc) events  | false
&nbsp;
**filtering**
filter                         | STRING            | filter rules, eg `exclude: db.*, include: *.tbl, include: *./bar(bar)?/, exclude: foo.bar.col=val` |
&nbsp;
**encryption**
encrypt                        | [ none &#124; data &#124; all ]     | encrypt mode: none = no encryption. "data": encrypt the `data` field only. `all`: encrypt entire maxwell message | none
secret_key                     | STRING                              | specify the encryption key to be used               | null
&nbsp;
**monitoring / metrics**
metrics_prefix           | STRING | the prefix maxwell will apply to all metrics | MaxwellMetrics
metrics_type             | [slf4j &#124; jmx &#124; http &#124; datadog]      | how maxwell metrics will be reported |
metrics_jvm              | BOOLEAN                             | enable jvm metrics: memory usage, GC stats, etc.| false
metrics_slf4j_interval   | SECONDS                             | the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured | 60
http_port                | INT                                 | the port the server will bind to when http reporting is configured | 8080
http_path_prefix         | STRING                              | http path prefix for the server | /
http_bind_address        | STRING                              | the address the server will bind to when http reporting is configured | all addresses
http_diagnostic          | BOOLEAN                             | enable http diagnostic endpoint | false
http_diagnostic_timeout  | MILLISECONDS                        | the http diagnostic response timeout| 10000
metrics_datadog_type     | [udp &#124; http] | when metrics_type includes `datadog` this is the way metrics will be reported, can only be one of [udp &#124; http] | udp
metrics_datadog_tags     | STRING | datadog tags that should be supplied, e.g. tag1:value1,tag2:value2 |
metrics_datadog_interval | INT | the frequency metrics are pushed to datadog, in seconds | 60
metrics_datadog_apikey   | STRING | the datadog api key to use when metrics_datadog_type = `http` |
metrics_datadog_host     | STRING | the host to publish metrics to when metrics_datadog_type = `udp` | localhost
metrics_datadog_port     | INT | the port to publish metrics to when metrics_datadog_type = `udp` | 8125
&nbsp;
**misc**
bootstrapper                   | [async &#124; sync &#124; none]                   | bootstrapper type.  See bootstrapping docs.        | async
init_position                  | FILE:POSITION[:HEARTBEAT]           | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties. |
replay                         | BOOLEAN                             | enable maxwell's read-only "replay" mode: don't store a binlog position or schema changes.  Not available in config.properties. |

<p id="sslopt" class="jumptarget">
SSL_OPTION: [ DISABLED &#124; PREFERRED &#124; REQUIRED &#124; VERIFY_CA &#124; VERIFY_IDENTITY ]
</p>
<p id="producer_type" class="jumptarget">
PRODUCER_TYPE: [ stdout &#124; file &#124; kafka &#124; kinesis &#124; pubsub &#124; sqs &#124; rabbitmq &#124; redis ]
</p>
<p id="jdbcopts" class="jumptarget">
DEFAULT_JDBC_OPTS: zeroDateTimeBehavior=convertToNull&amp;connectTimeout=5000
</p>
<p id="partition_by" class="jumptarget">
PARTITION_BY: [ database &#124; table &#124; primary_key &#124; column ]
</p>
<p id="partition_by_fallback" class="jumptarget">
PARTITION_BY_FALLBACK: [ database &#124; table &#124; primary_key ]
</p>
<p id="kafka_version" class="jumptarget">
KAFKA_VERSION: [ 0.8.2.2 &#124; 0.9.0.1 &#124; 0.10.0.1 &#124; 0.10.2.1 &#124; 0.11.0.1 ]
</p>
<p id="ack_timeout" class="jumptarget">
PRODUCER_ACK_TIMEOUT: In certain failure modes, async producers (kafka, kinesis, pubsub, sqs) may simply disappear
a message, never notifying maxwell of success or failure.  This timeout can be set as a heuristic; after this many
milliseconds, maxwell will consider an outstanding message lost and fail it.
</p>


### Configuration methods
***

Maxwell is configurable via the command-line, a properties file, or the environment.
The configuration priority is:

```
command line options > scoped env vars > properties file > default values
```

#### config.properties

Maxwell can be configured via a java properties file, specified via `--config`
or named "config.properties" in the current working directory.
Any command line options (except `init_position`, `replay`, `kafka_version` and
`daemon`) may be specified as "key=value" pairs.

#### via environment
If `env_config_prefix` given via command line or in `config.properties`, Maxwell
will configure itself with all environment variables that match the prefix. The
environment variable names are case insensitive.  For example, if maxwell is
started with `--env_config_prefix=FOO_` and the environment contains `FOO_USER=auser`,
this would be equivalent to passing `--user=auser`.


### Deployment scenarios
***

At a minimum, Maxwell needs row-level-replication turned on into order to
operate:

```
[mysqld]
server_id=1
log-bin=master
binlog_format=row
```

#### GTID support
As of 1.8.0, Maxwell contains support for
[GTID-based replication](https://dev.mysql.com/doc/refman/5.6/en/replication-gtids.html).  Enable it with the `--gtid_mode` configuration param.

Here's how you might configure your mysql server for GTID mode:

```
$ vi my.cnf

[mysqld]
server_id=1
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


#### RDS configuration
To run Maxwell against RDS, (either Aurora or Mysql) you will need to do the following:

- set binlog_format to "ROW".  Do this in the "parameter groups" section.  For a Mysql-RDS instance this parameter will be
  in a "DB Parameter Group", for Aurora it will be in a "DB Cluster Parameter Group".
- setup RDS binlog retention as described [here](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.MySQL.html).
  The tl;dr is to execute `call mysql.rds_set_configuration('binlog retention hours', 24)` on the server.

#### Split server roles

Maxwell uses MySQL for 3 different functions:

1. A host to store the captured schema in (`--host`).
2. A host to replicate from (`--replication_host`).
3. A host to capture the schema from (`--schema_host`).

Often, all three hosts are the same.  `host` and `replication_host` should be different
if mysql is chained off a slave.  `schema_host` should only be used when using the
maxscale replication proxy.

Note that bootstrapping is currently not available when `host` and
`replication_host` are separate, due to some implementation details.

#### Multiple Maxwell Instances

Maxwell can operate with multiple instances running against a single master, in
different configurations.  This can be useful if you wish to have producers
running in different configurations, for example producing different groups of
tables to different topics.  Each instance of Maxwell must be configured with a
unique `client_id`, in order to store unique binlog positions.

With MySQL 5.5 and below, each replicator (be it mysql, maxwell, whatever) must
also be configured with a unique `replica_server_id`.  This is a 32-bit integer
that corresponds to mysql's `server_id` parameter.  The value you configure
should be unique across all mysql and maxwell instances.

