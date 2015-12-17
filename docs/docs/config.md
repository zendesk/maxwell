<div class="content-title">Maxwell configuration</div>
***

### Command line options

option                                        | description | default
--------------------------------------------- | ----------- | -------
--config FILE                                 | location of `config.properties` file |
--log_level                                   | log level [DEBUG&#124;INFO &#124;WARN&#124;ERROR | INFO
--user USER                                   | mysql username |
--password PASSWORD                           | mysql password |
--host HOST                                   | mysql host |
--port PORT                                   | mysql port |
--producer PRODUCER                           | what type of producer to use: [stdout, kafka, file, profiler] | stdout
--output_file                                 | if using the file producer, write JSON rows to this path |
--include_dbs PATTERN                         | only send updates from these databases |
--exclude_dbs PATTERN                         | ignore updates from these databases |
--include_tables PATTERN                      | only send updates from tables named like PATTERN |
--exclude_tables PATTERN                      | ignore updates from tables named like PATTERN |
--kafka.bootstrap.servers                     | list of kafka brokers, listed as HOST:PORT[,HOST:PORT] |
--kafka_topic                                 | kafka topic to write to. | maxwell
--max_schemas                                 | how many old schemas maxwell should leave lying around in maxwell.schemas | 5
--init_position FILE:POSITION                 | ignore the information in maxwell.positions and start at the given binlog position. Not available in config.properties.
--replay                                      | enable maxwell's read-only "replay" mode.  Not available in config.properties.

### Properties file
***
If maxwell finds the file `config.properties` in $PWD it will use it.  Any
command line options (except init_position and replay) may be given as
"key=value" pairs.

Additionally, any configuration file options prefixed with 'kafka.' will be
passed into the kafka producer library, after having 'kafka.' stripped off the
front of the key.  So, for example if config.properties contains

```
kafka.batch.size=16384
```

Maxwell will send `batch.size=16384` to the kafka producer library.

### Filters
***
The options `include_dbs`, `exclude_dbs`, `include_tables`, and `exclude_tables` control whether
Maxwell will send an update for a given row to its producer.  All the options take a single value PATTERN,
which may either be a literal table/database name, given as `option=name`, or a regular expression,
given as `option=/regex/`.  The options are evaluated as follows:

1. only accept databases in `include_dbs` if non-empty
1. reject databases in `exclude_dbs`
1. only accept tables in `include_tables` if non-empty
1. reject tables in `exclude_tables`

So an example like `--include_dbs=/foo.*/ --exclude_tables=bar` will include `footy.zab` and exclude `footy.bar`


<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>

