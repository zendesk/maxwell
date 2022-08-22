# Kafka
***

The Kafka producer is perhaps the most production hardened of all the producers,
having run on high traffic instances at WEB scale.

## Topic
Maxwell writes to a kafka topic named "maxwell" by default. It is configurable
via `--kafka_topic`.  The given topic can be a plain string or a dynamic
string, e.g. `namespace_%{database}_%{table}`, where the topic will be
generated from data in the row.

## Client version
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


## Passing options to kafka
Any options present in `config.properties` that are prefixed with `kafka.` will
be passed into the Kafka producer library (with `kafka.` stripped off, see below for examples).  We use
the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)


## Example kafka configs

### Highest throughput

These properties would give high throughput performance.

```
kafka.acks = 1
kafka.compression.type = snappy
kafka.retries=0
```

### Most reliable

For at-least-once delivery, you will want something more like:

```
kafka.acks = all
kafka.retries = 5 # or some larger number
```

And you will also want to set `min.insync.replicas` on Maxwell's output topic.

## Key format

Maxwell generates keys for its Kafka messages based upon a mysql row's primary key in JSON format:

```
{ "database":"test_tb","table":"test_tbl","pk.id":4,"pk.part2":"hello"}
```

This key is designed to co-operate with Kafka's log compaction, which will save the last-known
value for a key, allowing Maxwell's Kafka stream to retain the last-known value for a row and act
as a source of truth.

# Partitioning
***

Both Kafka and AWS Kinesis support the notion of partitioned streams.
Because they like to make our lives hard, Kafka calls its two units "topics"
and "partitions", and Kinesis calls them "streams" and "shards.  They're the
same thing, though.  Maxwell is generally configured to write to N
partitions/shards on one topic/stream, and how it distributes to those N
partitions/shards can be controlled by `producer_partition_by`.

`producer_partition_by` gives you a choice of splitting your stream by database, table,
primary key, transaction id, column data, or "random".  How you choose
to partition your stream greatly influences the load and serialization properties
of your downstream consumers, so choose carefully.  A good rule of thumb is to
use the finest-grained partition scheme possible given serialization
constraints.


> *A brief partitioning digression:*

> If I were building, say, a simple search index of a single table, I might
> choose to partition by primary key; this would give you the best distribution
> of workload amongst your stream processors while maintaining a strict ordering
> of updates that happen to a certain row.

> If I were building something that needed better serialization properties --
let's say I needed to maintain strict ordering between updates that occured on
different tables -- I would drop back to partitioning by table or database.
This will reduce my throughput by a *lot* as a single stream consumer node will
end up will all the load for particular table/database, but I'm guaranteed that
the updates stay in order.

If you choose to partition by column data (that is, values inside columns in
your updates), you must set both:

- `producer_partition_columns` - a comma-separated list of column names, and
- `producer_partiton_by_fallback` - [_database_, _table_,
  _primary_key_] - this will be used as the partition key when the column does not
  exist.

When partitioning by column Maxwell will treat the values for the specified
columns as strings, concatenate them and use that value to partition the data.

### Kafka partitioning

A binlog event's partition is determined by the selected hash function and hash string as follows

```
  HASH_FUNCTION(producer_partion_value) % TOPIC.NUMBER_OF_PARTITIONS
```

The HASH_FUNCTION is either java's _hashCode_ or _murmurhash3_. The default
HASH_FUNCTION is _hashCode_. Murmurhash3 may be set with the
`kafka_partition_hash` option. The seed value for the murmurhash function is
hardcoded to 25342 in the MaxwellKafkaPartitioner class.  We tell you this
in case you need to reverse engineer where a row will land.

Maxwell will discover the number of partitions in its kafka topic upon boot.
This means that you should pre-create your kafka topics:

```
bin/kafka-topics.sh --zookeeper ZK_HOST:2181 --create \
                    --topic maxwell --partitions 20 --replication-factor 2
```

[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)



# Kinesis
***
## AWS Credentials
You will need to obtain an IAM user that has the following permissions for the stream you are planning on producing to:

- "kinesis:PutRecord"
- "kinesis:PutRecords"
- "kinesis:DescribeStream"

Additionally, the producer will need to be able to produce CloudWatch metrics which requires the following permission applied to the resource `*``:
- "cloudwatch:PutMetricData"

The resulting IAM policy document may look like this:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:PutRecord",
                "kinesis:PutRecords",
                "kinesis:DescribeStream"
            ],
            "Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```

See the [AWS docs](http://docs.aws.amazon.com/streams/latest/dev/controlling-access.html#kinesis-using-iam-examples) for the latest examples on which permissions are needed.

The producer uses the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) class to gain aws credentials.
See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

## Options
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

# SQS
***

## AWS Credentials
You will need to obtain an IAM user that has the permission to access the SQS service. The SQS producer also uses [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to get AWS credentials.

See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

In case you need to set up a different region also along with credentials then default one, see the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html#setup-credentials-setting-region).

## Options
Set the output queue in the `config.properties` by setting the following properites

- **sqs_signing_region**: the region to use for SigV4 signing of requests. e.g. `us-east-1`
- **sqs_service_endpoint**: the service endpoint either with or without the protocol (e.g. `https://sns.us-west-1.amazonaws.com` or `sns.us-west-1.amazonaws.com`)
- **sqs_queue_uri**: the full SQS queue uri from AWS console. e.g. `https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/maxwell`


The producer uses the [AWS SQS SDK](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQSClient.html).

# SNS
***

## AWS Credentials
You will need to obtain an IAM user that has the permission to access the SNS topic. The SNS producer also uses [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to get AWS credentials.

See the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) on how to setup the IAM user with the Default Credential Provider Chain.

In case you need to set up a different region also along with credentials then default one, see the [AWS docs](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html#setup-credentials-setting-region).

## Options
Set the topic arn in the `config.properties` by setting the `sns_topic` property to the topic name. FIFO topics should have a `.fifo` suffix. 

Optionally, you can enable `sns_attrs` to have maxwell attach various attributes to the message for subscription filtering. (Only `database` and `table` are currently supported)

The producer uses the [AWS SNS SDK](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sns/AmazonSNSClient.html).

# Nats
***
The configurable properties for nats are:

- `nats_url` - defaults to **nats://localhost:4222**
- `nats_subject` - defaults to **%{database}.%{table}**

`nats_subject` defines the Nats subject hierarchy to write to.  [Topic substitution](/producers#topic-substitution) is available.
All non-alphanumeric characters in the substitued values will be replaced by underscores.

# Google Cloud Pub/Sub
***
In order to publish to Google Cloud Pub/Sub, you will need to obtain an IAM service account that has been granted the `roles/pubsub.publisher` role.

See the Google Cloud Platform docs for the [latest examples of which permissions are needed](https://cloud.google.com/pubsub/docs/access_control), as well as [how to properly configure service accounts](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances).

Set the output stream in `config.properties` by setting the `pubsub_project_id` and `pubsub_topic` properties. Optionally configure a dedicated output topic
for DDL updates by setting the `ddl_pubsub_topic` property.

The producer uses the [Google Cloud Java Library for Pub/Sub](https://github.com/GoogleCloudPlatform/google-cloud-java/tree/master/google-cloud-pubsub) and uses its built-in configurations.

# Google Cloud BigQuery
***
To stream data into Google Cloud Bigquery, first there must be a table created on bigquery in order to stream the data
into defined as `bigquery_project_id.bigquery_dataset.bigquery_table`. The schema of the table must match the outputConfig. The column types should be defined as below

- database: string 
- table: string                                                                                                    
- type: string                                                                                                     
- ts: integer                                                                                                      
- xid: integer                                                                                                     
- xoffset: integer                                                                                                 
- commit: boolean                                                                                                  
- position: string                                                                                                 
- gtid: string                                                                                                     
- server_id: integer                                                                                               
- primary_key: string                                                                                              
- data: string                                                                                                     
- old: string

See the Google Cloud Platform docs for the [latest examples of which permissions are needed](https://cloud.google.com/bigquery/docs/access-control), as well as [how to properly configure service accounts](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances).

Set the output stream in `config.properties` by setting the `bigquery_project_id`, `bigquery_dataset` and `bigquery_table` properties.

The producer uses the [Google Cloud Java Bigquery Storage Library for Bigquery](https://github.com/googleapis/java-bigquerystorage) [Bigquery Storage Write API documenatation](https://cloud.google.com/bigquery/docs/write-api).
To use the Storage Write API, you must have `bigquery.tables.updateData` permissions.

This producer is using the Default Stream with at-least once semantics for greater data resiliency and fewer scaling restrictions

# RabbitMQ
***
To produce messages to RabbitMQ, you will need to specify a host in `config.properties` with `rabbitmq_host`. This is the only required property, everything else falls back to a sane default.

The remaining configurable properties are:

- `rabbitmq_user` - defaults to **guest**
- `rabbitmq_pass` - defaults to **guest**
- `rabbitmq_virtual_host` - defaults to **/**
- `rabbitmq_handshake_timeout` - defaults to **10000**
- `rabbitmq_exchange` - defaults to **maxwell**
- `rabbitmq_exchange_type` - defaults to **fanout**
- `rabbitmq_exchange_durable` - defaults to **false**
- `rabbitmq_exchange_autodelete` - defaults to **false**
- `rabbitmq_routing_key_template` - defaults to **%db%.%table%**
    - This config controls the routing key, where `%db%` and `%table%` are placeholders that will be substituted at runtime
- `rabbitmq_message_persistent` - defaults to **false**
- `rabbitmq_declare_exchange` - defaults to **true**

For more details on these options, you are encouraged to the read official RabbitMQ documentation here: [https://www.rabbitmq.com/documentation.html](https://www.rabbitmq.com/documentation.html)

# Redis
***

Choose type of redis data structure to create to by setting `redis_type` to one of:
`pubsub`, `xadd`, `lpush` or `rpush`.  The default is `pubsub`.

`redis_key` defaults to "maxwell" and supports [topic substitution](#topic-substitution)

Other configurable properties are:

- `redis_host` - defaults to **localhost**
- `redis_port` - defaults to **6379**
- `redis_auth` - defaults to **null**
- `redis_database` - defaults to **0**
- `redis_type` - defaults to **pubsub**
- `redis_key` - defaults to **maxwell**
- `redis_stream_json_key` - defaults to **message**
- `redis_sentinels` - doesn't have a default value
- `redis_sentinel_master_name` - doesn't have a default value

# Custom Producer
***
If none of the producers packaged with Maxwell meet your requirements, a custom producer can be added at runtime.  

In order to register your custom producer, you must implement the `ProducerFactory` interface, which is responsible for creating your custom `AbstractProducer`. Next, set the `custom_producer.factory` configuration property to your `ProducerFactory`'s fully qualified class name. Then add the custom `ProducerFactory` JAR and all its dependencies to the $MAXWELL_HOME/lib directory.

Your custom producer will likely require configuration properties as well. For that, use the `custom_producer.*` (or `CUSTOM_PRODUCER_*` if using env-variable configuration) property namespace. Those properties will be available to your producer via `MaxwellConfig.customProducerProperties`.

Custom producer factory and producer examples can be found here: [https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/producerfactory](https://github.com/zendesk/maxwell/tree/master/src/example/com/zendesk/maxwell/example/producerfactory)


# Topic substitution

Some producers may be given a template string from which they dynamically generate a topic (or whatever their equivalent of a kafka topic is).
Subsitutions are enclosed in by `%{}`.  The following substitutions are available:

- `%{database}`
- `%{table}`
- `%{type}` (insert/update/delete)

Topic substituion is available in the following producers:

- Kakfa, for topics
- Redis, for channels
- Nats, for subject heirarchies


