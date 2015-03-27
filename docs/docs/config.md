<div class="content-title">Maxwell configuration</div>

***

Maxwell can be configured by command line or a java "properties" file.

### Command line options

option                          | description
------------------------------- | -----------
--user USER                     | mysql username
--password PASSWORD             | mysql password
--host HOST                     | mysql host
--port PORT                     | mysql port
--producer stdout,kafka         | where shall we send these rows, sir?
--kafka.bootstrap.servers       | list of kafka nodes, listed as HOST:PORT[,HOST:PORT]
--kafka.maxwell.topic           | provide a topic for maxwell to write to. Default will be "maxwell".

### Configuration file options

If maxwell finds the file `config.properties` in $PWD it will use it.

option                        | description
----------------------------- | -----------
user=USER                     | mysql username
password=PASSWORD             | mysql password
host=HOST                     | mysql host
port=PORT                     | mysql port
producer=stdout,kafka,        | where shall we send these rows, sir?
kafka.maxwell.topic           | provide a topic for maxwell to write to. Default will be "maxwell".
kafka.*=XXX                   | any other options prefixed with 'kafka.' will be passed into the kafka producer library


### Kafka options

Any options in the configuration file prefixed with `kafka.` have that prefix stripped off, and passed directly
into the "new producer" configuration, as described here: [http://kafka.apache.org/documentation.html#newproducerconfigs](http://kafka.apache.org/documentation.html#newproducerconfigs)

Maxwell sets the following options by default, but you can override them in `config.properties`.

- (kafka.)acks = 1
- (kafka.)compression.type = gzip

### Topic and partitioning

Maxwell by default writes to the "maxwell" topic and if Kafka is set to autocreate topics on write, this will create the topic with exactly one partition.

If you need more control, you can precreate the topic with enough partitions and provide the topic name to maxwell using `--kafka.maxwell.topic` option.

Maxwell translates the database name into a partition number before writing to kafka. To have maximum parallelism, use as many partitions as you have databases.

The details of how to create a topic can be found here:

[http://kafka.apache.org/documentation.html#quickstart](http://kafka.apache.org/documentation.html#quickstart)

<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
