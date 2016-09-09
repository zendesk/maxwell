### Kafka options
***
Any options given to Maxwell that are prefixed with `kafka.` will be passed directly into the Kafka producer configuration
(with `kafka.` stripped off).  We use the "new producer" configuration, as described here:
[http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

Maxwell sets the following Kafka options by default, but you can override them in `config.properties`.

- kafka.acks = 1
- kafka.compression.type = gzip

Maxwell writes to a kafka topic named "maxwell" by default.  This can be changed with the `kafka_topic` option.

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
 
The HASH_STRING may be (_database_, _table_, _primary_key_).  The default HASH_STRING is the _database_. The partitioning field can be configured using the `kafka_partition_by` option.    

Maxwell will discover the number of partitions in its kafka topic upon boot.  This means that you should pre-create your kafka topics,
and with at least as many partitions as you have logical databases:

```
bin/kafka-topics.sh --zookeeper ZK_HOST:2181 --create \
                    --topic maxwell --partitions 20 --replication-factor 2
```


[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)


### Kafka 0.9
***
By default, maxwell runs with kafka clients 0.9.0.1, which isn't compatible withbrokers running kafka 0.8. The exception below will show in logs when that is the case:

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

There is a flag (--kafka0.8) that switches maxwell to run with kafka clients 0.8.2.2.

Note both kafka clients (0.8 and 0.9) are compatible with brokers running kafka 0.8.

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
