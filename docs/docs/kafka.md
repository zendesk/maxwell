### Kafka options

Any options in the configuration file prefixed with `kafka.` have that prefix stripped off, and passed directly
into the "new producer" configuration, as described here: [http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

Maxwell sets the following options by default, but you can override them in `config.properties`.

- (kafka.)acks = 1
- (kafka.)compression.type = gzip

### Topic and partitioning

Maxwell will write to the topic `maxwell` by default.  If Kafka is set to auto-create topics on write, this will create the topic with exactly one partition.

If you need more control, you can create the topic with enough partitions and provide the topic name to maxwell using `--kafka_topic` option.
The details of how to create a topic can be found here:

[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)

Maxwell translates the database name into a partition number before writing to kafka. Maxwell uses a simple bit of translation to get the partition number:

    Math.abs(dbName.hashCode() % numPartitions);

To have maximum parallelism while consuming, use as many partitions as you have databases.

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
