### Kafka options
***
Any options given to Maxwell that are prefixed with `kafka.` will be passed directly into the Kafka producer configuration
(with `kafka.` stripped off).  We use the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

Here's some decent kafka properties. You can set them in `config.properties`.

```
kafka.acks = 1
kafka.compression.type = snappy
kafka.metadata.fetch.timeout.ms=5000
kafka.retries=0
```

Note that these settings are optimized for throughput rather than full
consistency.  For at-least-once delivery, you will want something more like:

```
kafka.acks = all
kafka.retries = 5 # or some larger number
```

And you will also want to set `min.insync.replicas` on Maxwell's output topic.


### Kafka topic
***
Maxwell writes to a kafka topic named "maxwell" by default. It can be static,
e.g. 'maxwell', or dynamic, e.g. `namespace_%{database}_%{table}`. In the
latter case 'database' and 'table' will be replaced with the values for the row
being processed. This can be changed with the `kafka_topic` option.

### Kafka key
***
Maxwell generates keys for its Kafka messages based upon a mysql row's primary key in JSON format:

```
{ "database":"test_tb","table":"test_tbl","pk.id":4,"pk.part2":"hello"}
```

This key is designed to co-operate with Kafka's log compaction, which will save the last-known
value for a key, allowing Maxwell's Kafka stream to retain the last-known value for a row and act
as a source of truth.

### Kafka Partitioning
***
A binlog event's partition is determined by the selected hash function and hash string as follows

```
  HASH_FUNCTION(HASH_STRING) % TOPIC.NUMBER_OF_PARTITIONS
```

The HASH_FUNCTION is either java's _hashCode_ or _murmurhash3_. The default
HASH_FUNCTION is _hashCode_. Murmurhash3 may be set with the
`kafka_partition_hash` option. The seed value for the murmurhash function is
hardcoded to 25342 in the MaxwellKafkaPartitioner class.

The HASH_STRING may be (_database_, _table_, _primary_key_, _column_).  The
default HASH_STRING is the _database_. The partitioning field can be configured
using the `producer_partition_by` option.

When using `producer_partition_by`=_column_ you must set
`producer_partition_columns` with the column name(s) to partition by (e.g.
`producer_partition_columns`=user_id or
`producer_partition_columns`=user_id,create_date). You must also set
`kafka_partiton_by_fallback`. This may be (_database_, _table_, _primary_key_).
It is used when the column(s) specified does not exist in the current row. The
default is _database_.  When partitioning by _column_ Maxwell will treat the
values for the specified columns as strings, concatenate them and use that
value to partition the data. The above example, partitioning by user_id +
create_date would have a partition key similar to _1178532016-10-10 18:29:04_.

Maxwell will discover the number of partitions in its kafka topic upon boot.  This means that you should pre-create your kafka topics,
and with at least as many partitions as you have logical databases:

```
bin/kafka-topics.sh --zookeeper ZK_HOST:2181 --create \
                    --topic maxwell --partitions 20 --replication-factor 2
```


[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)


### Kafka client version
***
By default, maxwell runs with kafka clients 0.9.0.1. There is a flag (--kafka_version) that allows maxwell to run with either 0.8.2.2, 0.9.0.1, 0.10.0.1 or 0.10.1.0.
Noteables:
- Kafka clients 0.9.0.1 are not compatible with brokers running kafka 0.8. The exception below will show in logs when that is the case:

```
ERROR Sender - Uncaught error in kafka producer I/O thread:
SchemaException: Error reading field 'throttle_time_ms': java.nio.BufferUnderflowException
```

- Kafka clients 0.8 and 0.9 are compatible with brokers running kafka 0.8.
- 0.10.0.x clients only support 0.10.0.x or later brokers.
- Mixing Kafka 0.10 with other versions can lead to serious performance impacts.
  For More details, [read about it here](http://kafka.apache.org/0100/documentation.html#upgrade_10_performance_impact).

***

### Kinesis AWS credentials
***
You will need to obtain an IAM user that has the permission "kinesis:PutRecord" for the stream you are planning on producing to.
See the [AWS docs](http://docs.aws.amazon.com/streams/latest/dev/controlling-access.html#kinesis-using-iam-examples) for the latest examples on which permissions are needed.


The producer uses the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) class to gain aws credentials.
See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

### Kinesis Options
***
Set the output stream in `config.properties` by setting the `kinesis_stream` property.

The producer uses the [KPL (Kinesis Producer Library](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html) and uses the KPL built in configurations.
Copy `kinesis-producer-library.properties.example` to `kinesis-producer-library.properties` and configure the properties file to your needs.
The most important option here is configuring the region.
