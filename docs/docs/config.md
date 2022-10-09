# Reference
***

Configuration options are set either via command line or the "config.properties" file. 

##general

option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
config                         | STRING                              | location of `config.properties` file                | $PWD/config.properties
log_level                      | [LOG_LEVEL](#loglevel)              | log level                                           | info
daemon                         |                                     | running maxwell as a daemon                         |
env_config_prefix              | STRING                              | env vars matching prefix are treated as config values |

##mysql

option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
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
max_schemas                    | LONG                 | how many schema deltas to keep before triggering compaction operation | unlimited
binlog_heartbeat               | BOOLEAN              | enable binlog heartbeats to detect stale connections | DISABLED
&nbsp;
replication_host               | STRING               | server to replicate from.  See [split server roles](#split-server-roles) | *schema-store host*
replication_password           | STRING               | password on replication server                      | (none)
replication_port               | INT                  | port on replication server                          | 3306
replication_user               | STRING               | user on replication server                          |
replication_ssl                | [SSL_OPT](#sslopt)   | SSL behavior for replication cx cx                  | DISABLED
replication_jdbc_options       | STRING               | mysql jdbc connection options for replication server| [DEFAULT_JDBC_OPTS](#jdbcopts)
&nbsp;
schema_host                    | STRING               | server to capture schema from.  See [split server roles](#split-server-roles) | *schema-store host*
schema_password                | STRING               | password on schema-capture server                   | (none)
schema_port                    | INT                  | port on schema-capture server                       | 3306
schema_user                    | STRING               | user on schema-capture server                       |
schema_ssl                     | [SSL_OPT](#sslopt)   | SSL behavior for schema-capture server              | DISABLED
schema_jdbc_options            | STRING               | mysql jdbc connection options for schema server     | [DEFAULT_JDBC_OPTS](#jdbcopts)
&nbsp;

# producer options
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
producer                       | [PRODUCER_TYPE](#producer_type)     | type of producer to use                             | stdout
custom_producer.factory        | CLASS_NAME                          | fully qualified custom producer factory class, see [example](https://github.com/zendesk/maxwell/blob/master/src/example/com/zendesk/maxwell/example/producerfactory/CustomProducerFactory.java) |
producer_ack_timeout           | [PRODUCER_ACK_TIMEOUT](#ack_timeout) | time in milliseconds before async producers consider a message lost |
producer_partition_by          | [PARTITION_BY](#partition_by)       | input to kafka/kinesis partition function           | database
producer_partition_columns     | STRING                              | if partitioning by 'column', a comma separated list of columns |
producer_partition_by_fallback | [PARTITION_BY_FALLBACK](#partition_by_fallback) | required when producer_partition_by=column.  Used when the column is missing |
ignore_producer_error          | BOOLEAN              | When false, Maxwell will terminate on kafka/kinesis/pubsub publish errors (aside from RecordTooLargeException). When true, errors are only logged. See also dead_letter_topic | true

## file producer

option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
output_file                    | STRING                              | output file for `file` producer                     |
javascript                     | STRING                              | file containing javascript filters |



## kafka producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
kafka.bootstrap.servers        | STRING                              | kafka brokers, given as `HOST:PORT[,HOST:PORT]`     |
kafka_topic                    | STRING                              | kafka topic to write to.                            | maxwell
dead_letter_topic              | STRING                              | the topic to write a "skeleton row" (a row where `data` includes only primary key columns) when there's an error publishing a row. When `ignore_producer_error` is `false`, only RecordTooLargeException causes a fallback record to be published, since other errors cause termination. Currently only supported in Kafka publisher |
kafka_version                  | [KAFKA_VERSION](#kafka_version)     | run maxwell with specified kafka producer version.  Not available in config.properties. | 0.11.0.1
kafka_partition_hash           | [ default &#124; murmur3 ]          | hash function to use when choosing kafka partition   | default
kafka_key_format               | [ array &#124; hash ]               | how maxwell outputs kafka keys, either a hash or an array of hashes | hash
ddl_kafka_topic                | STRING                              | if output_ddl is true, kafka topic to write DDL changes to | *kafka_topic*

_See also:_ [Kafka Producer Documentation](/producers#kafka)


## kinesis producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
kinesis_stream                 | STRING                              | kinesis stream name |

_See also:_ [Kinesis Producer Documentation](/producers#kinesis)

## sqs producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
sqs_queue_uri                  | STRING                              | SQS Queue URI |

_See also:_ [SQS Producer Documentation](/producers#sqs)

## sns producer 
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
sns_topic                      | STRING                              | The SNS topic to publish to. FIFO topics should end with `.fifo` |
sns_attrs                      | STRING                              | Properties to set as attributes on the SNS message  |  

_See also:_ [SNS Producer Documentation](/producers#sns)

## nats producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
nats_url                       | STRING     | Comma separated list of nats urls.  may include [user:password style auth](https://docs.nats.io/developing-with-nats/security/userpass#connecting-with-a-user-password-in-the-url) | nats://localhost:4222
nats_subject                   | STRING     | Nats subject hierarchy.  [Topic substitution](/producers/#topic-substitution) available. | `%{database}.%{table}`

_See also:_ [Nats Producer Documentation](/producers#nats)

## pubsub producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
pubsub_topic                   | STRING     | Google Cloud pub-sub topic |
pubsub_platform_id             | STRING     | Google Cloud platform id associated with topic |
ddl_pubsub_topic               | STRING     | Google Cloud pub-sub topic to send DDL events to |
pubsub_request_bytes_threshold | LONG       | Set number of bytes until batch is send | 1
pubsub_message_count_batch_size| LONG       | Set number of messages until batch is send | 1
pubsub_message_ordering_key    | STRING     | Google Cloud pub-sub ordering key template (also enables message ordering when set) |
pubsub_publish_delay_threshold | LONG       | Set time passed in millis until batch is send | 1
pubsub_retry_delay             | LONG       | Controls the delay in millis before sending the first retry message | 100
pubsub_retry_delay_multiplier  | FLOAT      | Controls the increase in retry delay per retry | 1.3
pubsub_max_retry_delay         | LONG       | Puts a limit on the value in seconds of the retry delay | 60
pubsub_initial_rpc_timeout     | LONG       | Controls the timeout in seconds for the initial RPC | 5
pubsub_rpc_timeout_multiplier  | FLOAT      | Controls the change in RPC timeout | 1.0
pubsub_max_rpc_timeout         | LONG       | Puts a limit on the value in seconds of the RPC timeout | 600
pubsub_total_timeout           | LONG       | Puts a limit on the value in seconds of the retry delay, so that the RetryDelayMultiplier can't increase the retry delay higher than this amount | 600
pubsub_emulator                | STRING     | Google Cloud pub-sub emulator host to send events to |

_See also:_ [PubSub Producer Documentation](/producers#google-cloud-pubsub)

## bigquery producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
bigquery_project_id            | STRING     | Google Cloud bigquery project id |
bigquery_dataset               | STRING     | Google Cloud bigquery dataset id |
bigquery_table                 | STRING     | Google Cloud bigquery table id |

_See also:_ [PubSub Producer Documentation](/producers#google-cloud-bigquery)

## rabbitmq producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
rabbitmq_user                  | STRING     | Username of Rabbitmq connection | guest
rabbitmq_pass                  | STRING     | Password of Rabbitmq connection | guest
rabbitmq_host                  | STRING     | Host of Rabbitmq machine
rabbitmq_port                  | INT        | Port of Rabbitmq machine |
rabbitmq_virtual_host          | STRING     | Virtual Host of Rabbitmq |
rabbitmq_handshake_timeout     | STRING     | Handshake timeout of Rabbitmq connection in milliseconds |
rabbitmq_exchange              | STRING     | Name of exchange for rabbitmq publisher |
rabbitmq_exchange_type         | STRING     | Exchange type for rabbitmq |
rabbitmq_exchange_durable      | BOOLEAN    | Exchange durability. | false
rabbitmq_exchange_autodelete   | BOOLEAN    | If set, the exchange is deleted when all queues have finished using it. | false
rabbitmq_routing_key_template  | STRING     | A string template for the routing key, `%db%` and `%table%` will be substituted. | `%db%.%table%`.
rabbitmq_message_persistent    | BOOLEAN    | Eanble message persistence. | false
rabbitmq_declare_exchange      | BOOLEAN    | Should declare the exchange for rabbitmq publisher | true

_See also:_ [RabbitMQ Producer Documentation](/producers#rabbitmq)

## redis producer
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
redis_host                     | STRING                   | Host of Redis server | localhost
redis_port                     | INT                      | Port of Redis server | 6379
redis_auth                     | STRING                   | Authentication key for a password-protected Redis server
redis_database                 | INT                      | Database of Redis server | 0
redis_type                     | [ pubsub &#124; xadd &#124; lpush &#124; rpush ]  | Selects either Redis Pub/Sub, Stream, or List. | pubsub
redis_key                      | STRING                   | Redis channel/key for Pub/Sub, XADD or LPUSH/RPUSH | maxwell
redis_stream_json_key          | STRING                   | Redis XADD Stream Message Field Name | message
redis_sentinels                | STRING                   | Redis sentinels list in format host1:port1,host2:port2,host3:port3... Must be only used with redis_sentinel_master_name
redis_sentinel_master_name     | STRING                   | Redis sentinel master name. Must be only used with redis_sentinels

_See also:_ [Redis Producer Documentation](/producers#redis)


# formatting
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
output_binlog_position         | BOOLEAN  | records include binlog position     | false
output_gtid_position           | BOOLEAN  | records include gtid position, if available | false
output_commit_info             | BOOLEAN  | records include commit and xid      | true
output_xoffset                 | BOOLEAN  | records include virtual tx-row offset | false
output_push_timestamp          | BOOLEAN  | records are timestamped with a high-precision value before being sent to the producer | false
output_nulls                   | BOOLEAN  | records include fields with NULL values    | true
output_server_id               | BOOLEAN  | records include server_id                  | false
output_thread_id               | BOOLEAN  | records include thread_id                  | false
output_schema_id               | BOOLEAN  | records include schema_id, schema_id is the id of the latest schema tracked by maxwell and doesn't relate to any mysql tracked value                  | false
output_row_query               | BOOLEAN  | records include INSERT/UPDATE/DELETE statement. Mysql option "binlog_rows_query_log_events" must be enabled | false
output_primary_keys            | BOOLEAN  | DML records include list of values that make up a row's primary key | false
output_primary_key_columns     | BOOLEAN  | DML records include list of columns that make up a row's primary key | false
output_ddl                     | BOOLEAN  | output DDL (table-alter, table-create, etc) events  | false
output_null_zerodates          | BOOLEAN  | should we transform '0000-00-00' to null? | false
output_naming_strategy         | STRING   | naming strategy of field name of JSON. can be `underscore_to_camelcase` | none

# filtering
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
filter                         | STRING            | filter rules, eg `exclude: db.*, include: *.tbl, include: *./bar(bar)?/, exclude: foo.bar.col=val` |

_See also:_ [filtering](/filtering)

# encryption
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
encrypt                        | [ none &#124; data &#124; all ]     | encrypt mode: none = no encryption. "data": encrypt the `data` field only. `all`: encrypt entire maxwell message | none
secret_key                     | string                              | specify the encryption key to be used               | null

# high availability
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
ha                             |                                     | enable maxwell client HA                            |
jgroups_config                 | string                              | location of xml configuration file for jGroups      | $PWD/raft.xml
raft_member_id                 | string                              | uniquely identify this node within jgroups-raft cluster |

_See also:_ [High Availability](/high_availability)

# monitoring / metrics
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
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
metrics_age_slo     | INT | Latency service level objective threshold in seconds (Optional). When set, a `message.publish.age.slo_violation` metric is emitted to Datadog if the latency exceeds the threshold |
metrics_datadog_interval | INT | the frequency metrics are pushed to datadog, in seconds | 60
metrics_datadog_apikey   | STRING | the datadog api key to use when metrics_datadog_type = `http` |
metrics_datadog_site     | STRING | the site to publish metrics to when metrics_datadog_type = `http` | us
metrics_datadog_host     | STRING | the host to publish metrics to when metrics_datadog_type = `udp` | localhost
metrics_datadog_port     | INT | the port to publish metrics to when metrics_datadog_type = `udp` | 8125

_See also:_ [Monitoring](/monitoring)

# misc
option                         | argument                            | description                                         | default
-------------------------------|-------------------------------------| --------------------------------------------------- | -------
bootstrapper                   | [async &#124; sync &#124; none]                   | bootstrapper type.  See [bootstrapping docs](/bootstrapping).        | async
init_position                  | FILE:POSITION[:HEARTBEAT]           | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties. [see note](/deployment#-init_position)|
replay                         | BOOLEAN                             | enable maxwell's read-only "replay" mode: don't store a binlog position or schema changes.  Not available in config.properties. |
buffer_memory_usage            | FLOAT                               | Determines how much memory the Maxwell event buffer will use from the jvm max memory. Size of the buffer is: buffer_memory_usage * -Xmx" | 0.25
http_config                    | BOOLEAN                             | enable http config endpoint for config updates without restart | false
binlog_event_queue_size        | INT                                 | Size of queue to buffer events parsed from binlog   | 5000


<p id="loglevel" class="jumptarget">
LOG_LEVEL: [ debug &#124; info &#124; warn &#124; error ]
</p>
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
PARTITION_BY: [ database &#124; table &#124; primary_key &#124; transaction_id &#124; column &#124; random ]
</p>
<p id="partition_by_fallback" class="jumptarget">
PARTITION_BY_FALLBACK: [ database &#124; table &#124; primary_key &#124; transaction_id ]
</p>
<p id="kafka_version" class="jumptarget">
KAFKA_VERSION: [ 0.8.2.2 &#124; 0.9.0.1 &#124; 0.10.0.1 &#124; 0.10.2.1 &#124; 0.11.0.1 ]
</p>
<p id="ack_timeout" class="jumptarget">
PRODUCER_ACK_TIMEOUT: In certain failure modes, async producers (kafka, kinesis, pubsub, sqs) may simply disappear
a message, never notifying maxwell of success or failure.  This timeout can be set as a heuristic; after this many
milliseconds, maxwell will consider an outstanding message lost and fail it.
</p>


# Configuration methods
***

Maxwell is configurable via the command-line, a properties file, or the environment.
The configuration priority is:

```
command line options > scoped env vars > properties file > default values
```

## config.properties

Maxwell can be configured via a java properties file, specified via `--config`
or named "config.properties" in the current working directory.
Any command line options (except `init_position`, `replay`, `kafka_version` and
`daemon`) may be specified as "key=value" pairs.

## via environment
If `env_config_prefix` given via command line or in `config.properties`, Maxwell
will configure itself with all environment variables that match the prefix. The
environment variable names are case insensitive.  For example, if maxwell is
started with `--env_config_prefix=FOO_` and the environment contains `FOO_USER=auser`,
this would be equivalent to passing `--user=auser`.

## via PATCH: /config
If `http_config` is set to true in config.properties or in the environment,
the endpoint /config will be exposed. Currently only filter updates are supported,
and a filter can be updated with a request in the following format

`PATCH: /config`
```json
{
	"filter": "exclude: noisy_db.*"
}
```

A get request will return the live config state
`GET: /config`
```json
{
	"filter": "exclude: noisy_db.*"
}
```


