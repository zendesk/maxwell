### Kafka options
***
Any options given to Maxwell that are prefixed with `kafka.` will be passed directly into the Kafka producer configuration
(with `kafka.` stripped off).  We use the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

These are recommended properties. You can remove or update them in `config.properties`.
- kafka.acks = 1
- kafka.compression.type = snappy
- kafka.metadata.fetch.timeout.ms=5000
- kafka.retries=0

Maxwell writes to a kafka topic named "maxwell" by default. It can be static, e.g. 'maxwell', or dynamic, e.g. `namespace_%{database}_%{table}`. In the latter case 'database' and 'table' will be replaced with the values for the row being processed. This can be changed with the `kafka_topic` option.

### Kafka key
***
Maxwell generates keys for its Kafka messages based upon a mysql row's primary key in JSON format:

```
{ "database":"test_tb","table":"test_tbl","pk.id":4,"pk.part2":"hello"}
```

This key is designed to co-operate with Kafka's log compaction, which will save the last-known
value for a key, allowing Maxwell's Kafka stream to retain the last-known value for a row and act
as a source of truth.

### Partitioning
***
A binlog event's partition is determined by the selected hash function and hash string as follows

```
  HASH_FUNCTION(HASH_STRING) % TOPIC.NUMBER_OF_PARTITIONS
```

The HASH_FUNCTION is either java's _hashCode_ or _murmurhash3_. The default HASH_FUNCTION is _hashCode_. Murmurhash3 may be set with the `kafka_partition_hash` option. The seed value for the murmurhash function is hardcoded to 25342 in the MaxwellKafkaPartitioner class.
 
The HASH_STRING may be (_database_, _table_, _primary_key_, _column_).  The default HASH_STRING is the _database_. The partitioning field can be configured using the `kafka_partition_by` option.

When using `kafka_partition_by`=_column_ you must set `kafka_partition_columns` with the column name(s) to partition by (e.g. `kafka_partition_columns`=user_id or `kafka_partition_columns`=user_id,create_date). You must also set `kafka_partiton_by_fallback`. This may be (_database_, _table_, _primary_key_). It is used when the column(s) specified does not exist in the current row. The default is _database_.
When partitioning by _column_ Maxwell will treat the values for the specified columns as strings, concatenate them and use that value to partition the data. The above example, partitioning by user_id + create_date would have a partition key similar to _1178532016-10-10 18:29:04_.

Maxwell will discover the number of partitions in its kafka topic upon boot.  This means that you should pre-create your kafka topics,
and with at least as many partitions as you have logical databases:

```
bin/kafka-topics.sh --zookeeper ZK_HOST:2181 --create \
                    --topic maxwell --partitions 20 --replication-factor 2
```


[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)


### Kafka 0.9
***
By default, maxwell runs with kafka clients 0.9.0.1. There is a flag (--kafka_version) that allows maxwell to run with either 0.8.2.2, 0.9.0.1, 0.10.0.1 or 0.10.1.0.
Noteables:
- Kafka clients 0.9.0.1 are not compatible with brokers running kafka 0.8. The exception below will show in logs when that is the case:

```
14:53:06,033 ERROR Sender - Uncaught error in kafka producer I/O thread: 
org.apache.kafka.common.protocol.types.SchemaException: Error reading field 'throttle_time_ms': java.nio.BufferUnderflowException
    at org.apache.kafka.common.protocol.types.Schema.read(Schema.java:71) ~[kafka-clients-0.9.0.1.jar:?]
    at org.apache.kafka.clients.NetworkClient.handleCompletedReceives(NetworkClient.java:439) ~[kafka-clients-0.9.0.1.jar:?]
    at org.apache.kafka.clients.NetworkClient.poll(NetworkClient.java:265) ~[kafka-clients-0.9.0.1.jar:?]
    at org.apache.kafka.clients.producer.internals.Sender.run(Sender.java:216) ~[kafka-clients-0.9.0.1.jar:?]
    at org.apache.kafka.clients.producer.internals.Sender.run(Sender.java:128) [kafka-clients-0.9.0.1.jar:?]
    at java.lang.Thread.run(Thread.java:745) [?:1.7.0_79]
```

- Kafka clients 0.8 and 0.9 are compatible with brokers running kafka 0.8.
- 0.10.0.x clients only support 0.10.0.x or later brokers.
- Mixing Kafka 0.10 with other versions can lead to serious performance impacts:
http://kafka.apache.org/0100/documentation.html#upgrade_10
> Notes to clients with version 0.9.0.0: Due to a bug introduced in 0.9.0.0, clients that depend on ZooKeeper (old Scala high-level Consumer and MirrorMaker if used with the old consumer) will not work with 0.10.0.x brokers. Therefore, 0.9.0.0 clients should be upgraded to 0.9.0.1 before brokers are upgraded to 0.10.0.x. This step is not necessary for 0.8.X or 0.9.0.1 clients.
http://kafka.apache.org/0100/documentation.html#upgrade_10_performance_impact
> The message format in 0.10.0 includes a new timestamp field and uses relative offsets for compressed messages. The on disk message format can be configured through log.message.format.version in the server.properties file. The default on-disk message format is 0.10.0. If a consumer client is on a version before 0.10.0.0, it only understands message formats before 0.10.0. In this case, the broker is able to convert messages from the 0.10.0 format to an earlier format before sending the response to the consumer on an older version. However, the broker can't use zero-copy transfer in this case. Reports from the Kafka community on the performance impact have shown CPU utilization going from 20% before to 100% after an upgrade, which forced an immediate upgrade of all clients to bring performance back to normal. To avoid such message conversion before consumers are upgraded to 0.10.0.0, one can set log.message.format.version to 0.8.2 or 0.9.0 when upgrading the broker to 0.10.0.0. This way, the broker can still use zero-copy transfer to send the data to the old consumers. Once consumers are upgraded, one can change the message format to 0.10.0 on the broker and enjoy the new message format that includes new timestamp and improved compression. The conversion is supported to ensure compatibility and can be useful to support a few apps that have not updated to newer clients yet, but is impractical to support all consumer traffic on even an overprovisioned cluster. Therefore it is critical to avoid the message conversion as much as possible when brokers have been upgraded but the majority of clients have not.
> For clients that are upgraded to 0.10.0.0, there is no performance impact.
> Note: By setting the message format version, one certifies that all existing messages are on or below that message format version. Otherwise consumers before 0.10.0.0 might break. In particular, after the message format is set to 0.10.0, one should not change it back to an earlier format as it may break consumers on versions before 0.10.0.0.

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
