### Kafka
***

#### Topic
Maxwell writes to a kafka topic named "maxwell" by default. It can be static,
e.g. 'maxwell', or dynamic, e.g. `namespace_%{database}_%{table}`. In the
latter case 'database' and 'table' will be replaced with the values for the row
being processed. This can be changed with the `kafka_topic` option.

#### Client version
By default, maxwell uses the kafka 1.0.0 library. The `--kafka_version` flag
lets you choose an alternate library version: 0.8.2.2, 0.9.0.1, 0.10.0.1, 0.10.2.1 or
0.11.0.1, 1.0.0.  This flag is only available on the command line.


- The 0.8.2.2 client is only compatible with brokers running kafka 0.8.
- The 0.10.0.x client is only compatible with brokers 0.10.0.x or later.
- Mixing the 0.10 client with other versions can lead to serious performance impacts.
  For More details, [read about it here](http://kafka.apache.org/0100/documentation.html#upgrade_10_performance_impact).
- The 0.11.0 client can talk to version 0.10.0 or newer brokers.
- The 0.9.0.1 client is not compatible with brokers running kafka 0.8. The exception below will show in logs when that is the case:

```
ERROR Sender - Uncaught error in kafka producer I/O thread:
SchemaException: Error reading field 'throttle_time_ms': java.nio.BufferUnderflowException
```


#### Extended options
Any options given to Maxwell that are prefixed with `kafka.` will be passed directly into the Kafka producer configuration
(with `kafka.` stripped off).  We use the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)


#### Performant properties

These properties would give high throughput performance.

```
kafka.acks = 1
kafka.compression.type = snappy
kafka.retries=0
```

#### At-least-once properties

For at-least-once delivery, you will want something more like:

```
kafka.acks = all
kafka.retries = 5 # or some larger number
```

And you will also want to set `min.insync.replicas` on Maxwell's output topic.

#### Keys

Maxwell generates keys for its Kafka messages based upon a mysql row's primary key in JSON format:

```
{ "database":"test_tb","table":"test_tbl","pk.id":4,"pk.part2":"hello"}
```

This key is designed to co-operate with Kafka's log compaction, which will save the last-known
value for a key, allowing Maxwell's Kafka stream to retain the last-known value for a row and act
as a source of truth.

The JSON-hash based key format is tricky to regenerate in a stable fashion.  If you have an
application in which you need to parse and re-generate keys, it's advised you enable
`--kafka_key_format=array`, which will generate kafka keys that can be parsed and re-output byte-for-byte.

### Partitioning
***

Both Kafka and AWS Kinesis support the notion of partitioned streams.
Partitioning is controlled by `producer_partition_by`, which gives you the
option to split your stream by database, table, primary
key, or column data.  How you choose to partition will influence the consumers
of the maxwell stream.  A good rule of thumb is to use the finest-grained
partition scheme possible given serialization constraints.

To partition by column data, you must set both:

- `producer_partition_columns`, a comma-separated list of column names, and
- `producer_partiton_by_fallback`. This may be (_database_, _table_,
  _primary_key_), and will be used as a default value when the column does not
  exist.

When partitioning by column Maxwell will treat the values for the specified
columns as strings, concatenate them and use that value to partition the data.

#### Kafka partitioning

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

Maxwell will discover the number of partitions in its kafka topic upon boot.  This means that you should pre-create your kafka topics,
and with at least as many partitions as you have logical databases:

```
bin/kafka-topics.sh --zookeeper ZK_HOST:2181 --create \
                    --topic maxwell --partitions 20 --replication-factor 2
```


[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)

### Kinesis
***
#### AWS Credentials
You will need to obtain an IAM user that has the following permissions for the stream you are planning on producing to:

- "kinesis:PutRecord"
- "kinesis:PutRecords"
- "kinesis:DescribeStream"
- "cloudwatch:PutMetricData"

See the [AWS docs](http://docs.aws.amazon.com/streams/latest/dev/controlling-access.html#kinesis-using-iam-examples) for the latest examples on which permissions are needed.


The producer uses the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) class to gain aws credentials.
See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

#### Options
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

### SQS
***

#### AWS Credentials
You will need to obtain an IAM user that has the permission to access the SQS service. The SQS producer also uses [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to get AWS credentials.

See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

In case you need to set up a different region also along with credentials then default one, see the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html#setup-credentials-setting-region).

#### Options
Set the output queue in the `config.properties` by setting the `sqs_queue_uri` property to full SQS queue uri from AWS console.

The producer uses the [AWS SQS SDK](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQSClient.html).


### Google Cloud Pub/Sub
***
In order to publish to Google Cloud Pub/Sub, you will need to obtain an IAM service account that has been granted the `roles/pubsub.publisher` role.

See the Google Cloud Platform docs for the [latest examples of which permissions are needed](https://cloud.google.com/pubsub/docs/access_control), as well as [how to properly configure service accounts](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances).

Set the output stream in `config.properties` by setting the `pubsub_project_id` and `pubsub_topic` properties. Optionally configure a dedicated output topic
for DDL updates by setting the `ddl_pubsub_topic` property.

The producer uses the [Google Cloud Java Library for Pub/Sub](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-pubsub) and uses its built-in configurations.

### RabbitMQ
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
- `rabbitmq_declare_exchange` - defaults to **true**

For more details on these options, you are encouraged to the read official RabbitMQ documentation here: https://www.rabbitmq.com/documentation.html

### Redis
***
Set the output stream in `config.properties` by setting the `redis_pub_channel` property for redis_type = pubsub or set the `redis_list_key` property when using redis_type = lpush.

Other configurable properties are:

- `redis_host` - defaults to **localhost**
- `redis_port` - defaults to **6379**
- `redis_auth` - defaults to **null**
- `redis_database` - defaults to **0**
- `redis_type` - defaults to **pubsub**
- `redis_list_key` - defaults to **maxwell**

### Custom Producer
***
If none of the producers packaged with Maxwell meet your requirements, a custom producer can be added at runtime. The producer is responsible for processing the raw database rows. Note that your producer may receive DDL and heartbeat rows as well, but your producer can easily filter them out (see example).

In order to register your custom producer, you must implement the `ProducerFactory` interface, which is responsible for creating your custom `AbstractProducer`. Next, set the `custom_producer.factory` configuration property to your `ProducerFactory`'s fully qualified class name. Then add the custom `ProducerFactory` and all its dependencies to the $MAXWELL_HOME/lib directory.

Your custom producer will likely require configuration properties as well. For that, use the `custom_producer.*` property namespace. Those properties will be exposed to your producer via `MaxwellConfig.customProducerProperties`.

Custom producer factory and producer examples can be found here: [https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/producerfactory](https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/producerfactory)
