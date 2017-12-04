### Kafka options
***
Any options given to Maxwell that are prefixed with `kafka.` will be passed directly into the Kafka producer configuration
(with `kafka.` stripped off).  We use the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

Here's some decent kafka properties. You can set them in `config.properties`.

```
kafka.acks = 1
kafka.compression.type = snappy
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
By default, maxwell runs with kafka clients 0.11.0.1. There is a flag (--kafka_version) that allows maxwell to run with either 0.8.2.2, 0.9.0.1, 0.10.0.1, 0.10.2.1 or 0.11.0.1.
Noteables:
- Kafka clients 0.9.0.1 are not compatible with brokers running kafka 0.8. The exception below will show in logs when that is the case:

```
ERROR Sender - Uncaught error in kafka producer I/O thread:
SchemaException: Error reading field 'throttle_time_ms': java.nio.BufferUnderflowException
```

- Kafka clients 0.8 are compatible with brokers running kafka 0.8.
- 0.10.0.x clients only support 0.10.0.x or later brokers.
- Mixing Kafka 0.10 with other versions can lead to serious performance impacts.
  For More details, [read about it here](http://kafka.apache.org/0100/documentation.html#upgrade_10_performance_impact).
- 0.11.0 clients can talk to version 0.10.0 or newer brokers.

***

### Kinesis AWS credentials
***
You will need to obtain an IAM user that has the following permissions for the stream you are planning on producing to:

- "kinesis:PutRecord"
- "kinesis:PutRecords"
- "kinesis:DescribeStream"
- "cloudwatch:PutMetricData"

See the [AWS docs](http://docs.aws.amazon.com/streams/latest/dev/controlling-access.html#kinesis-using-iam-examples) for the latest examples on which permissions are needed.


The producer uses the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) class to gain aws credentials.
See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

### Kinesis Options
***
Set the output stream in `config.properties` by setting the `kinesis_stream` property.

The producer uses the [KPL (Kinesis Producer Library)](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html) and uses the KPL built in configurations.
Copy `kinesis-producer-library.properties.example` to `kinesis-producer-library.properties` and configure the properties file to your needs.

You are **required** to configure the region. For example:

```
# set explicitly
Region=us-west-2
# or set with an environment variable
Region=$AWS_DEFAULT_REGION
```

By default, the KPL implements [record aggregation](http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html#w2ab1c12b7b7c19c11), which usually increases producer throughput by allowing you to increase the number of records sent per API call. However, aggregated records are encoded differently (using Google Protocol Buffers) than records that are not aggregated. Therefore, if you are not using the [KCL (Kinesis Client Library)](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html) to consume records (for example, you are using AWS Lambda) you will need to either disaggregate the records in your consumer (for example, by using the [AWS Kinesis Aggregation library](https://github.com/awslabs/kinesis-aggregation)), or disable record aggregation in your `kinesis-producer-library.properties` configuration.

To disable aggregation, add the following to your configuration:

```
AggregationEnabled=false
```

Remember: if you disable record aggregation, you will lose the benefit of potentially greater producer throughput.

### Google Cloud Pub/Sub Options
***
In order to publish to Google Cloud Pub/Sub, you will need to obtain an IAM service account that has been granted the `roles/pubsub.publisher` role.

See the Google Cloud Platform docs for the [latest examples of which permissions are needed](https://cloud.google.com/pubsub/docs/access_control), as well as [how to properly configure service accounts](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances).

### Google Cloud Pub/Sub Options
***
Set the output stream in `config.properties` by setting the `pubsub_project_id` and `pubsub_topic` properties. Optionally configure a dedicated output topic
for DDL updates by setting the `ddl_pubsub_topic` property.

The producer uses the [Google Cloud Java Library for Pub/Sub](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-pubsub) and uses its built-in configurations.

### RabbitMQ Options
***
To produce messages to RabbitMQ, you will need to specify a host in `config.properties` with `rabbitmq_host`. This is the only required property, everything else falls back to a sane default.

The remaining configurable properties are:
- `rabbitmq_user` - defaults to **guest**
- `rabbitmq_pass` - defaults to **guest**
- `rabbitmq_virtual_host` - defaults to **/**
- `rabbitmq_exchange` - defaults to **maxwell**
- `rabbitmq_exchange_type` - defaults to **fanout**
- `rabbitmq_exchange_durable` - defaults to **false**
- `rabbitmq_exchange_autodelete` - defaults to **false**
- `rabbitmq_routing_key_template` - defaults to **%db%.%table%**
    - This config controls the routing key, where `%db%` and `%table%` are placeholders that will be substituted at runtime
- `rabbitmq_message_persistent` - defaults to **false**

For more details on these options, you are encouraged to the read official RabbitMQ documentation here: https://www.rabbitmq.com/documentation.html

### Redis Options
***
Set the output stream in `config.properties` by setting the `redis_pub_channel` property.

Other configurable properties are:

- `redis_host` - defaults to **localhost**
- `redis_port` - defaults to **6379**
- `redis_auth` - defaults to **null**
- `redis_database` - defaults to **0**

### Graylog Options
***
The configurable properties are:

- `graylog_host` - defaults to **localhost**
- `graylog_port` - defaults to **12201**
- `graylog_transport` - defaults to **udp**
- `graylog_additional_field.*` - optional to add additional field 