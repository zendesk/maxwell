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
--kafka_topic                   | provide a topic for maxwell to write to. Default will be "maxwell".

### Configuration file options

If maxwell finds the file `config.properties` in $PWD it will use it.

option                        | description
----------------------------- | -----------
user=USER                     | mysql username
password=PASSWORD             | mysql password
host=HOST                     | mysql host
port=PORT                     | mysql port
producer=stdout,kafka,        | where shall we send these rows, sir?
kafka.*=XXX                   | any options prefixed with 'kafka.' will be passed into the kafka producer library
kafka_topic                   | provide a topic for maxwell to write to. Default will be "maxwell".


<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
