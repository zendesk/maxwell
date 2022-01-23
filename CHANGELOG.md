# Maxwell changelog

### [v1.36.0](https://github.com/zendesk/maxwell/releases/tag/v1.36.0): 
_2022-01-23_

- fix bug where the millionth binlog would kinda sort "overflow" and the
  binlog positions would stop moving.
- My benefactor here asked that I stopped creating cute release names.
  The security department, mysteriously.


### [v1.35.5](https://github.com/zendesk/maxwell/releases/tag/v1.35.5): 
_2021-12-29_

- log4j, again and agian.


### [v1.35.4](https://github.com/zendesk/maxwell/releases/tag/v1.35.4): 
_2021-12-18_

- log4j turns 2.17.0, happy birthday


### [v1.35.3](https://github.com/zendesk/maxwell/releases/tag/v1.35.3): 
_2021-12-15_

- log4j vulnerability #2


### [v1.35.2](https://github.com/zendesk/maxwell/releases/tag/v1.35.2): 
_2021-12-12_

- better logging when we can't connect on startup


### [v1.35.1](https://github.com/zendesk/maxwell/releases/tag/v1.35.1): 
_2021-12-10_

- log4j upgrade to upgrade past the giant security hole


### [v1.35.0](https://github.com/zendesk/maxwell/releases/tag/v1.35.0): 
_2021-11-30_

- couple of parser fixes
- docker builds are now multi-platform
- replication_reconnection_retries configuration option
- quote table names in bootstrapper properly


### [v1.34.1](https://github.com/zendesk/maxwell/releases/tag/v1.34.1): 
_2021-09-21_

- support for mysql 8's visible/invisible columns
- support mariadb's if-exists/if-not-exists for partition management
- add an index for the http endpoint


### [v1.34.0](https://github.com/zendesk/maxwell/releases/tag/v1.34.0): 
_2021-07-29_

- intern a bunch of objects in our in-memory representation of schema.
  Saves gobs of memory in cases where one has N copies of the same
  database.  Note that this changes the API of Columns, should any
  embedded Maxwell application be using that.
- go up to BIGINT for maxwell's auto-increment ids


### [v1.33.1](https://github.com/zendesk/maxwell/releases/tag/v1.33.1): 
_2021-06-02_

- properties may now be fetched from a javascript blob in the env
- RowMap provides access to primary keys
- fix an odd NPE in mariaDB init


### [v1.33.0](https://github.com/zendesk/maxwell/releases/tag/v1.33.0): 
_2021-03-29_

- Add HTTP endpoint for runtime reconfiguration


### [v1.32.0](https://github.com/zendesk/maxwell/releases/tag/v1.32.0): 
_2021-03-17_

- Amazon SNS producer added, thanks Rober Wittman
- kafka 2.7.0 supported
- stackdriver metrics logging available


### [v1.31.0](https://github.com/zendesk/maxwell/releases/tag/v1.31.0): 
_2021-02-11_

- Add producer for NATS streaming server


### [v1.30.0](https://github.com/zendesk/maxwell/releases/tag/v1.30.0): 
_2021-02-05_

- support server-sent heartbeating on the binlog connection via --binlog-heartbeat
- can connect to rabbitmq by URL, supports SSL connections
- fix parser bug with multiline SQL
- target JDK 11 -- we have dropped support for JDK 8
- ability to send a microsecond timestamp via --output_push_timestamp
- fixes for odd azure mysql connection failures


### [v1.29.2](https://github.com/zendesk/maxwell/releases/tag/v1.29.2): 
_2021-01-27_

- fix for terrible performance regression in bootstrapping


### [v1.29.1](https://github.com/zendesk/maxwell/releases/tag/v1.29.1): 
_2020-12-23_

- small bugfix release, fixes binlog event type processing in mysql 8


### [v1.29.0](https://github.com/zendesk/maxwell/releases/tag/v1.29.0): 
_2020-12-15_

- High Availability support via jgroups-raft
- rework --help text


### [v1.28.2](https://github.com/zendesk/maxwell/releases/tag/v1.28.2): 
_2020-12-02_

- fix for encryption parsing error on table creation
- some logging around memory usage in RowMapBuffer


### [v1.28.1](https://github.com/zendesk/maxwell/releases/tag/v1.28.1): 
_2020-11-25_

- fix http server issue in 1.28.0


### [v1.28.0](https://github.com/zendesk/maxwell/releases/tag/v1.28.0): 
_2020-11-19_

- schema compaction!  with the new --max_schemas option, maxwell will
  periodically roll up the `maxwell`.`schemas` table, preventing it from
  growing infinitely long.
- fix metricsAgeSloMS calculation
- support SRID columns
- fix parsing of complex INDEX(CAST()) statements
- various dependency bumps


### [v1.27.1](https://github.com/zendesk/maxwell/releases/tag/v1.27.1): 
_2020-08-07_

- redis producer gets sentinal support
- fix a double-reconnect race condition
- file producer honors javascript row-suppression
- better error messaging when we lack REPLICATION SLAVE privs
- miscellaneous dependency bumps


### [v1.27.0](https://github.com/zendesk/maxwell/releases/tag/v1.27.0): 
_2020-06-30_

- better support for empty/null passwords
- allow bootstrap utility to query replication_host
- a few library upgrades, notably pubsub and kinesis library
- bootstrap connection uses jdbc_options properly
- add logging for when we hit out of sync schema exceptions
- allow for partitioning by thread_id, thx @gogov
- fresh and clean documentation


### [v1.26.4](https://github.com/zendesk/maxwell/releases/tag/v1.26.4): 
_2020-06-08_

 - support now() function with precision


### [v1.26.3](https://github.com/zendesk/maxwell/releases/tag/v1.26.3): 
_2020-05-26_

- use pooled redis connections, fixes corruption when redis was accessed
from multiple threads (bootstrap/producer), thanks @lucastex
- fix date handling of '0000-01-01'
- fix race condition in binlog reconnect logic


### [v1.26.2](https://github.com/zendesk/maxwell/releases/tag/v1.26.2): 
_2020-05-18_

- bootstraps can be scheduled in the future by setting the `started_at`
  column, thanks @lucastex
- two mysql 8 fixes; one for a `DEFAULT(function())` parse error, one
  for supporting DEFAULT ENCRYPTION


### [v1.26.1](https://github.com/zendesk/maxwell/releases/tag/v1.26.1): 
_2020-05-07_

- fixes for redis re-connection login, thanks much @lucastex


### [v1.26.0](https://github.com/zendesk/maxwell/releases/tag/v1.26.0): 
_2020-05-06_

- We now support mysql 8's caching_sha2_password authentication scheme
- support for converting JSON field names to camelCase


### [v1.25.3](https://github.com/zendesk/maxwell/releases/tag/v1.25.3): 
_2020-05-02_

- fixes memory leak in mysql-binlog-connector
- fixes exceptions that occur when a connection passes wait_timeout


### [v1.25.2](https://github.com/zendesk/maxwell/releases/tag/v1.25.2): 
_2020-05-01_

- Fixes for a long standing JSON bug in 8.0.19+


### [v1.25.1](https://github.com/zendesk/maxwell/releases/tag/v1.25.1): 
_2020-04-22_

- issue #1457, ALTER DATABASE with implicit database name
- maxwell now runs on JDK 11 in docker
- exit with status 2 when we can't find binlog files


### [v1.25.0](https://github.com/zendesk/maxwell/releases/tag/v1.25.0): 
_2020-03-29_

- swap un-maintained snaq.db with C3P0.
- support eu datadog metrics
- protect against lost connections during key queries (bootstrapping,
      heartbeats, postition setting)


### [v1.24.2](https://github.com/zendesk/maxwell/releases/tag/v1.24.2): 
_2020-03-25_

- bugfix parsing errors: compressed columns, exchange partitions,
  parenthesis-enclosed default values, `drop column foo.t`.
- add partition-by-random feature.
- update jackson-databind to get security patch
- fix redis channel interpolation on RPUSH


### [v1.24.1](https://github.com/zendesk/maxwell/releases/tag/v1.24.1): 
_2020-01-21_

- allow jdbc_options on secondary connections
- fix a crash in bootstrapping / javascript filters
- fix a regression in message.publish.age metric


### [v1.24.0](https://github.com/zendesk/maxwell/releases/tag/v1.24.0): 
_2019-12-14_

 - add comments field to bootstrapping, thanks Tom Collins
 - fix sql bug with #comments style comments


### [v1.23.5](https://github.com/zendesk/maxwell/releases/tag/v1.23.5): 
_2019-12-12_

 - Update bootstrap documentation
 - Bump drop wizard metrics to support Java versions 10+


### [v1.23.4](https://github.com/zendesk/maxwell/releases/tag/v1.23.4): 
_2019-12-03_

- Bump and override dependencies to fix security vulnerabilities.
- Update redis-key config options

 - list changes


### [v1.23.3](https://github.com/zendesk/maxwell/releases/tag/v1.23.3): 
_2019-11-21_

- pubsubDelayMultiplier may now be 1.0
- allow %{database} and %{topic} interpolation into redis producer
- docs updates
- setup default client_id in maxwell-bootstrap util


### [v1.23.2](https://github.com/zendesk/maxwell/releases/tag/v1.23.2): 
_2019-10-18_

- upgrade jackson
- stop passing maxwell rows through the JS filter.  too dangerous.


### [v1.23.1](https://github.com/zendesk/maxwell/releases/tag/v1.23.1): 
_2019-10-12_

- Add option for XADD (redis streams) operation
- Add configuration flag for tuning transaction buffer memory
- sectionalize help text


### [v1.23.0](https://github.com/zendesk/maxwell/releases/tag/v1.23.0): 
_2019-10-08_

- Added AWS FIFO support
- Add retry and batch settings to pubs producer
- Add support for age SLO metrics


### [v1.22.6](https://github.com/zendesk/maxwell/releases/tag/v1.22.6): 
_2019-09-20_

- upgrade mysql-connector-java to 8.0.17
- use a newer docker image as base
 - list changes


### [v1.22.5](https://github.com/zendesk/maxwell/releases/tag/v1.22.5): 
_2019-09-06_

- bugfix for bootstrapping off a split replica that doesn't contain a
  "maxwell" database
- Fix a parser issue with db.table.column style column names


### [v1.22.4](https://github.com/zendesk/maxwell/releases/tag/v1.22.4): 
_2019-08-23_

 - Add row type to fallback message
 - Upgrade jackson-databind


### [v1.22.3](https://github.com/zendesk/maxwell/releases/tag/v1.22.3): 
_2019-06-20_

- fix issue with google pubsub in 1.22.2


### [v1.22.2](https://github.com/zendesk/maxwell/releases/tag/v1.22.2): 
_2019-06-18_

- fix an issue with bootstrapping-on-replicas
- add --output_primary_keys and --output_primary_key_columns
- fix a very minor memory leak with blacklists


### [v1.22.1](https://github.com/zendesk/maxwell/releases/tag/v1.22.1): 
_2019-05-28_

- fix crash in rabbit-mq producer
- better support for maxwell + azure-mysql
- remove bogus different-host bootstrap check
- some security upgrades


### [v1.22.0](https://github.com/zendesk/maxwell/releases/tag/v1.22.0): 
_2019-04-16_

- Bootstrapping has been reworked and is now available in all setups,
including those in which the maxwell store is split from the replicator.
- cleanup and fix a deadlock in the kafka fallback queue logic
- add .partition_string = to javascript filters


### [v1.21.1](https://github.com/zendesk/maxwell/releases/tag/v1.21.1): 
_2019-03-29_

- Upgrade binlog connector.  Should fix issues around deserialization
errors.


### [v1.21.0](https://github.com/zendesk/maxwell/releases/tag/v1.21.0): 
_2019-03-23_

- Bootstrapping output no longer contain binlog positions.  Please update
  any code that relies on this.
- Fix 3 parser issues.


### [v1.20.0](https://github.com/zendesk/maxwell/releases/tag/v1.20.0): 
_2019-02-28_

- add support for partitioning by transaction ID thx @hexene
- add support for a kafka "fallback" topic to write to
  when a message fails to write
- add UJIS charset support
- parser bug: multiple strings concatenate to make one default string
- parser bug: deal with bizarre column renames which are then referenced
  in AFTER column statements


### [v1.19.7](https://github.com/zendesk/maxwell/releases/tag/v1.19.7): 
_2019-01-25_

- fix a parser error with empty sql comments
- interpret latin-1 as windows-1252, not iso-whatever, thx @borleaandrei


### [v1.19.6](https://github.com/zendesk/maxwell/releases/tag/v1.19.6): 
_2019-01-20_

- Further fixes for GTID-reconnection issues.
- Crash sanely when GTID-enabled maxwell is connected to clearly the wrong master,
  thanks @acampoh


### [v1.19.5](https://github.com/zendesk/maxwell/releases/tag/v1.19.5): 
_2019-01-15_

- Fixes for unreliable connections wrt to GTID events; previously we
  restart in any old position, now we throw away the current transaction
  and restart the replicator again at the head of the GTID event.


### [v1.19.4](https://github.com/zendesk/maxwell/releases/tag/v1.19.4): 
_2019-01-12_

- Fixes for a maxwell database not making it through the blacklist
- Add `output_null_zerodates` parameter to control how we treat
  '0000-00-00'


### [v1.19.3](https://github.com/zendesk/maxwell/releases/tag/v1.19.3): 
_2018-12-19_

- Add a universal backpressure mechanism.  This should help people who
were running into out-of-memory situations while bootstrapping.


### [v1.19.2](https://github.com/zendesk/maxwell/releases/tag/v1.19.2): 
_2018-12-02_

- Include schema_id in bootstrap events
- add more logging around binlog connector losing connection
- add retry logic to redis
- some aws fixes
- allow pushing JS hashes/arrays into data from js filters

 - list changes


### [v1.19.1](https://github.com/zendesk/maxwell/releases/tag/v1.19.1): 
_2018-11-12_

- Handle mysql bit literals in DEFAULT statements
- blacklist out CREATE ROLE etc
- upgrade dependencies to pick up security issues


### [v1.19.0](https://github.com/zendesk/maxwell/releases/tag/v1.19.0): 
_2018-10-27_

- mysql 8 support!
- utf8 enum values are supported now
- fix #1125, bootstrapping issue for TINYINT(1)
- fix #1145, nasty bug around SQL blacklist and columns starting with "begin"
- only resume bootstraps that are targeted at this client_id
- fixes for blacklists and heartbeats.  Did I ever mention blacklists
  are a terrible idea?


### [v1.18.0](https://github.com/zendesk/maxwell/releases/tag/v1.18.0): 
_2018-09-15_

- memory optimizations for large schemas (especially shareded schemas with lots of duplicates)
- add support for an http endpoint to support Prometheus metrics
- allow javascript filters to access the row query object
- javascript filters now run in the bootstrap process
- support for non-latin1 column names
- add `--output_schema_id` option
- better handling of packet-too-big errors from Kinesis
- add message.publish.age metric


### [v1.17.1](https://github.com/zendesk/maxwell/releases/tag/v1.17.1): 
_2018-07-03_

- fix a regression around filters + bootstrapping
- fix a regression around filters + database-only-ddl


### [v1.17.0](https://github.com/zendesk/maxwell/releases/tag/v1.17.0): 
_2018-06-28_

v1.17.0 brings a new level of configurability by allowing you to inject
a bit of javascript into maxwell's processing.  Should be useful!  Also:

- fix regression for Alibaba RDS tables


### [v1.16.1](https://github.com/zendesk/maxwell/releases/tag/v1.16.1): 
_2018-06-21_

- Fix Bootstrapping for JSON columns
- add --recapture_schema flag for when ya wanna start over
- add kafka 1.0 libraries, make them default


### [v1.16.0](https://github.com/zendesk/maxwell/releases/tag/v1.16.0): 
_2018-06-15_

v1.16.0 brings a rewrite of Maxwell's filtering system, giving it a
concise list of rules that are executed in sequence.  It's now possible
to exclude tables from a particular database, exclude columns matching a
value, and probably some other use cases.
See http://maxwells-daemon.io/config/#filtering for details.


### [v1.15.0](https://github.com/zendesk/maxwell/releases/tag/v1.15.0): 
_2018-06-13_

This is a bug-fix release, but it's big enough I'm giving it a minor
version.

- Fix a very old bug in which DDL rows were writing the *start* of the
row into `maxwell.positions`, leading to chaos in some scenarios where
maxwell managed to stop on the row and double-process it, as well as to
a few well-meaning patches.
- Fix the fact that maxwell was outputting "next-position" instead of
"position" of a row into JSON.
- Fix the master-recovery code to store schema that corresponds to the
start of a row, and points the replicator at the next-position.

Much thanks to Tim, Likun and others in sorting this mess out.


### [v1.14.7](https://github.com/zendesk/maxwell/releases/tag/v1.14.7): 
_2018-06-03_

- add RowMap#getRowQuery, thx @saimon7
- revert alpine-linux docker image fiasco
- fix RawJSONString not serializable, thx @niuhaifeng


### [v1.14.6](https://github.com/zendesk/maxwell/releases/tag/v1.14.6): 
_2018-05-15_

- Fix docker image


### [v1.14.5](https://github.com/zendesk/maxwell/releases/tag/v1.14.5): 
_2018-05-15_

- reduce docker image footprint
- add benchmarking framework
- performance improvements for date/datetime columns
- fix parser error on UPGRADE PARTITIONING


### [v1.14.4](https://github.com/zendesk/maxwell/releases/tag/v1.14.4): 
_2018-05-07_

 - Fix race condition in SchemaCapturer


### [v1.14.3](https://github.com/zendesk/maxwell/releases/tag/v1.14.3): 
_2018-05-04_

- Enable jvm metrics

### [v1.14.2](https://github.com/zendesk/maxwell/releases/tag/v1.14.2): 
_2018-05-02_

- fix regression in 1.14.1 around bootstrapping host detection
- fix heartbeating code around table includes


### [v1.14.1](https://github.com/zendesk/maxwell/releases/tag/v1.14.1): 
_2018-05-01_

- bootstraps can now take a client_id
- improved config validation for embedded mode


### [v1.14.0](https://github.com/zendesk/maxwell/releases/tag/v1.14.0): 
_2018-04-24_

- new feature `--output_xoffset` to uniquely identify rows within transactions,
  thx Jens Gyti
- Bug fixes around "0000-00-00" times.
- Bug fixes around dates pre 1000 AD


### [v1.13.5](https://github.com/zendesk/maxwell/releases/tag/v1.13.5): 
_2018-04-11_

- Support environment variable based configuration

### [v1.13.4](https://github.com/zendesk/maxwell/releases/tag/v1.13.4): 
_2018-04-03_

- Added possibility to do not declare the rabbitmq exchange.

### [v1.13.3](https://github.com/zendesk/maxwell/releases/tag/v1.13.3): 
_2018-04-03_


 - Add logging for binlog errors
 - Maven warning fix
 - Do not include current position DDL schema to avoid processing DDL twice
 - Always write null fields in primary key fields
 - Bugfix: fix http_path_prefix command line option issue

### [v1.13.2](https://github.com/zendesk/maxwell/releases/tag/v1.13.2): 
_2018-03-06_

- fix a bug with CHARACTER SET = DEFAULT
- maxwell now eclipse-friendly.
- configurable bind-address for maxwell's http server


### [v1.13.1](https://github.com/zendesk/maxwell/releases/tag/v1.13.1): 
_2018-02-20_

- redis producer now supports LPUSH, thx @m-denton
- RowMap can now contain artbitrary attributes for embedded maxwell, thx @jkgeyti
- bugfix: fix jdbc option parsing when value contains `=`
- bugfix: apparently the SQS producer was disabled
- bugfix: fix a situation where adding a second client could cause
  schemas to become out of sync
- support for --daemon


### [v1.13.0](https://github.com/zendesk/maxwell/releases/tag/v1.13.0): 
_2018-02-01_

- proper SSL connection support, thanks @cadams5
- support for including original SQL in insert/update/deletes, thanks @saimon7
- fixes for float4, float8 and other non-mysql datatypes
- bump kinesis lib to 0.12.8
- fix for bug when two databases share a single table


### [v1.12.0](https://github.com/zendesk/maxwell/releases/tag/v1.12.0): 
_2018-01-09_

- Support for injecting a custom producer, thanks @tomcollinsproject
- New producer for Amazon SQS, thanks @vikrant2mahajan
- Maxwell can now filter rows based on column values, thanks @finnplay
- Fixes for the Google Pubsub producer (it was really broken), thanks @finnplay
- DDL output can now optionally include the source SQL, thanks @sungjuly
- Support for double-quoted table/database/etc names
- rabbitmq option for persistent messages, thanks @d-babiak
- SQL parser bugfix for values like +1.234, thanks @hexene


### [v1.11.0](https://github.com/zendesk/maxwell/releases/tag/v1.11.0): 
_2017-11-22_

     - default kafka client upgrades to 0.11.0.1
     - fix the encryption issue (https://github.com/zendesk/maxwell/issues/803)


### [v1.10.9](https://github.com/zendesk/maxwell/releases/tag/v1.10.9): 
_2017-10-30_

We recommend all v1.10.7 and v1.10.8 users upgrade to v1.10.9.

 - Add missing Kafka clients
 - Listen and report on binlog connector lifecycle events for better visibility
 - Reduce docker image size


### [v1.10.8](https://github.com/zendesk/maxwell/releases/tag/v1.10.8): 
_2017-10-12_

 - Fix docker builds
 - Add Google Cloud Pub/Sub producer
 - RabbitMQ producer enhancements


### [v1.10.7](https://github.com/zendesk/maxwell/releases/tag/v1.10.7): 
_2017-10-11_

- Java 8 upgrade
- Diagnostic health check endpoint
- Encryption
- Documentation update: encryption, kinesis producer, schema storage fundamentals, etc.

### [v1.10.6](https://github.com/zendesk/maxwell/releases/tag/v1.10.6): 
_2017-08-14_

 - Binlog-connector upgrade
 - Bug-fix: when using literal string for an option that accepts Regex, Regex characters are no longer special
 - If master recovery is enabled, Maxwell cleans up old positions for the same server and client id


### [v1.10.5](https://github.com/zendesk/maxwell/releases/tag/v1.10.5): 
_2017-07-25_

- Shyko's binlog-connector is now the default and only replication
backend available for maxwell.


### [v1.10.4](https://github.com/zendesk/maxwell/releases/tag/v1.10.4): 
_2017-07-10_

Notable changes:

 - Shutdown hardening. If maxwell can't shut down (because the kafka
   producer is in a bad state and `close()` never terminates, for example),
   it would previously stall and process no messages. Now, shutdown is run
   in a separate thread and there is an additional watchdog thread which
   forcibly kills the maxwell process if it can't shut down within 10
   seconds.
 - Initial support for running maxwell from java, rather then as its own
   process. This mode of operation is still experimental, but we'll
   accept PRs to improve it (thanks Geoff Lywood).
 - Fix incorrect handling of negative (pre-epoch dates) when using
   binlog_connector mode (thanks Geoff Lywood).


### [v1.10.3](https://github.com/zendesk/maxwell/releases/tag/v1.10.3): 
_2017-06-06_

 - tiny release to fix a units error in the `replication.lag` metric
   (subtracting seconds from milliseconds)


### [v1.10.2](https://github.com/zendesk/maxwell/releases/tag/v1.10.2): 
_2017-06-04_

- added metrics: "replication.queue.time" and "inflightmessages.count"
- renamed "time.overall" metric to "message.publish.time"
- documentation updates (thanks Chintan Tank)


### [v1.10.1](https://github.com/zendesk/maxwell/releases/tag/v1.10.1): 
_2017-05-26_

The observable changes in this minor release are a new configuration for Kafka/Kinesis producer to abort processing on publish errors, and support of Kafka 0.10.2. Also a bunch of good refactoring has been done for heartbeat processing. List of changes:   

- Support Kafka 0.10.2   
- Stop procesing RDS hearbeats   
- Keep maxwell heartbeat going every 10 seconds when database is quiet   
- Allow for empty double-quoted string literals for database schema changes   
- Ignore Kafka/Kinesis producer errors based on new configuration ignore_producer_error

### [v1.10.0](https://github.com/zendesk/maxwell/releases/tag/v1.10.0): 
_2017-05-09_

This is a small release, primarily around a change to how schemas are
stored. Maxwell now stores the `last_heartbeat_read` with each entry
in the `schemas` table, making schema management more resilient to
cases where binlog numbers are reused, but means that you must take
care if you need to roll back to an earlier version. If you deploy
v1.10.0, then roll back to an earlier version, you should manually
update all `schemas`.`last_heartbeat_read` values to `0` before
redeploying v1.10.0 or higher.

Other minor changes:

  - allow negative default numbers in columns
  - only store final binlog position if it has changed
  - blacklist internal aurora table `rds_heartbeat*'
  - log4j version bump (allows for one entry per line JSON logging)


### [v1.9.0](https://github.com/zendesk/maxwell/releases/tag/v1.9.0): 
_2017-04-26_

Maxwell 1.9 adds one main feature: monitoring support, contributed by
Scott Ferguson. Multiple backends can be configured, read the updated
docs for full details.

There's also some bugfixes:

- filter DDL messages based on config
- determine newest schema from binlog order, not creation order
- add task manager to shutdown cleanly on error
- minor logging improvements


### [v1.8.2](https://github.com/zendesk/maxwell/releases/tag/v1.8.2): 
_2017-04-11_

Bugfix release.

- maxwell would crash on a quoted partition name
- fixes for alters on non-string tables containing VARCHAR
- use seconds instead of milliseconds for DDL messages


### [v1.8.1](https://github.com/zendesk/maxwell/releases/tag/v1.8.1): 
_2017-02-20_

- performance improves in capturing and restoring schema, thx Joren
  Minnaert
- Allow for capturing from a separate mysql host (adds support for using
  Maxscale as a replication proxy), thx Adam Szkoda

### [v1.8.0](https://github.com/zendesk/maxwell/releases/tag/v1.8.0): 
_2017-02-14_

In version 1.8.0 Maxwell gains alpha support for GTID-based positions!
All praise due to Henry Cai.

### [v1.7.2](https://github.com/zendesk/maxwell/releases/tag/v1.7.2): 
_2017-01-30_

- Fix a bug found where maxwell could cache the wrong TABLE_MAP_ID for a
  binlog event, leading to crashes or in some cases data mismatches.

### [v1.7.1](https://github.com/zendesk/maxwell/releases/tag/v1.7.1): 
_2017-01-24_

- bootstrapping now can take a `--where` clause
- performance improvements in the kafka producer

### [v1.7.0](https://github.com/zendesk/maxwell/releases/tag/v1.7.0): 
_2017-01-07_

Maxwell 1.7 brings 2 major new, alpha features.  The first is Mysql 5.7
support, including JSON column type support and handling of 5.7 SQL, but
_not_ including GTID support yet.  This is based on porting Maxwell to
Stanley Shyko's binlog-connector library.  Thanks to Stanley for his
amazing support doing this port.

The second major new feature is a producer for Amazon's Kinesis streams,
This was contributed in full by the dogged and persistent Thomas Dziedzic.
Check it out with `--producer=kinesis`.

There's also some bugfixes:
- Amazon RDS heartbeat events now tick maxwell's position, thx Scott Ferguson
- allow CHECK() statements inside column definitions

### [v1.6.0](https://github.com/zendesk/maxwell/releases/tag/v1.6.0): 
_2016-12-29_

This is mostly a bugfix release, but it gets a minor version bump due to
a single change of behavior: dates and timestamps which mysql may
accept, but are considered invalid (0000-00-00 is a notable example)
previously had inconsistent behavior.  Now we convert these to NULL.
Other bugfixes:
- heartbeats have moved into their own table
- more fixes around alibaba rds
- ignore DELETE statements that are output for MEMORY tables upon server
  restart
- allow pointing maxwell to a pre-existing database

### [v1.5.2](https://github.com/zendesk/maxwell/releases/tag/v1.5.2): 
_2016-12-07_

- add support for kafka 0.10.1 @ smferguson
- master recovery: cleanup positions from previous master; prevent
  errors on flip-back.
- fix a bug that would trigger in certain cases when dropping a column
  that was part of the primary-key

### [v1.5.1](https://github.com/zendesk/maxwell/releases/tag/v1.5.1): 
_2016-11-24_

This is a bugfix release.
- fixes for bootstrapping with an alternative maxwell-schema name and an
  `include_database` filter, thanks Lucian Jones
- fixes for kafka 0.10 with lz4 compression, thanks Scott Ferguson
- ignore the RDS table `mysql.ha_health_check` table
- Get the bootstrapping process to output NULL values.
- fix a quoting issue in the bootstrap code, thanks @mylesjao.

### [v1.5.0](https://github.com/zendesk/maxwell/releases/tag/v1.5.0): 
_2016-11-07_

- CHANGE: Kafka producer no longer ships with hard-coded defaults.
  Please ensure you have "compression.type", "metadata.fetch.timeout.ms", and "retries"
  configured to your liking.
- bugfix: fix a regression in handling `ALTER TABLE change c int after b` statements
- warn on servers with missing server_id

### [v1.4.2](https://github.com/zendesk/maxwell/releases/tag/v1.4.2): 
_2016-11-01_

- kafka 0.10.0 support, as well as a re-working of the --kafka_version
  command line option.

### [v1.4.1](https://github.com/zendesk/maxwell/releases/tag/v1.4.1): 
_2016-10-27_

- support per-table topics, Thanks @smferguson and @sschatts.
- fix a parser issue with DROP COLUMN CASCADE, thanks @smferguson

### [v1.4.0](https://github.com/zendesk/maxwell/releases/tag/v1.4.0): 
_2016-10-21_

1.4.0 brings us two nice new features:
- partition-by-column: see --kafka_partition_columns.  Thanks @smferguson
- output schema changes as JSON: see --output_ddl.  Thanks @xmlking
- As well as a fix around race conditions on shutdown.

### [v1.3.0](https://github.com/zendesk/maxwell/releases/tag/v1.3.0): 
_2016-10-03_

- support for fractional DATETIME, TIME, TIMESTAMP columns, thanks @Dagnan
- support for outputting server_id & thread_id, thanks @sagiba
- fix a race condition in bootstrap support

### [v1.2.2](https://github.com/zendesk/maxwell/releases/tag/v1.2.2): 
_2016-09-23_

- Maxwell will now include by default fields with NULL values (as null
  fields).  To disable this and restore the old functionality where fields
  were omitted, pass `--output_nulls=false`
- Fix an issue with multi-client support where two replicators would
  ping-pong heartbeats at each other
- Fix an issue where a client would attempt to recover a position from a
  mismatched client_id
- Fix a bug when using CHANGE COLUMN on a primary key

### [v1.2.1](https://github.com/zendesk/maxwell/releases/tag/v1.2.1): 
_2016-09-15_

This is a bugfix release.
- fix a parser bug around ALTER TABLE CHARACTER SET
- fix bin/maxwell to pull in the proper version of the kafka-clients
  library

### [v1.2.0](https://github.com/zendesk/maxwell/releases/tag/v1.2.0): 
_2016-09-12_

1.2.0 is a major release of Maxwell that introduces master recovery
features; when a slave is promoted to master, Maxwell is now capable of
recovering the position.  See the `--master_recovery` flag for more
details.

It also upgrades the kafka producer library to 0.9.  If you're using
maxwell with a kafka 0.8 server, you must now pass the `--kafka0.8` flag
to maxwell.

### [v1.1.6](https://github.com/zendesk/maxwell/releases/tag/v1.1.6): 
_2016-09-07_

- minor bugfix in which maxwell with --replay mode was trying to write
  heartbeats

### [v1.1.5](https://github.com/zendesk/maxwell/releases/tag/v1.1.5): 
_2016-09-04_

- @dadah89 adds --output_binlog_position to optionally output the
  position with the row
- @dadah89 adds --output_commit_info to turn off xid/commit fields
- maxwell now supports tables with partitions
- maxwell now supports N maxwells per-server.  see the client_id /
  replica_server_id options.
- two parser fixes, for engine=`innodb` and CHARSET ASCII
- lay the ground work for doing master recovery; we add a heartbeat into
  the positions table that we can co-ordinate around.

### [v1.1.4](https://github.com/zendesk/maxwell/releases/tag/v1.1.4): 
_2016-08-05_

- add support for a bunch more charsets (gbk, big5, notably)
- fix Maxwell's handling of kafka errors - previously we were trying to
  crash Maxwell by throwing a RuntimeException out of the Kafka
  Producer, but this was a failure.  Now we log and skip all errors.

### [v1.1.3](https://github.com/zendesk/maxwell/releases/tag/v1.1.3): 
_2016-07-14_

This is a bugfix release, which fixes:
- https://github.com/zendesk/maxwell/issues/376, a problem parsing
  RENAME INDEX
- https://github.com/zendesk/maxwell/issues/371, a problem with the
  SERIAL datatype
- https://github.com/zendesk/maxwell/issues/362, we now preserve the
  original casing of columns
- https://github.com/zendesk/maxwell/issues/373, we were incorrectly
  expecting heartbeats to work under 5.1

### [v1.1.2](https://github.com/zendesk/maxwell/releases/tag/v1.1.2): 
_2016-06-27_

- pick up latest mysql-connector-j, fixes #369
- fix an issue where maxwell could skip ahead positions if a leader failed.
- rework buffering code to be much kinder to the GC and JVM heap in case
  of very large transactions / rows inside transactions
- kinder, gentler help text when you specify an option incorrectly

### [v1.1.1](https://github.com/zendesk/maxwell/releases/tag/v1.1.1): 
_2016-05-23_

- fixes a race condition setting the binlog position that would get
  maxwell stuck

### [v1.1.0](https://github.com/zendesk/maxwell/releases/tag/v1.1.0): 
_2016-05-20_

- much more efficient processing of schema updates storage, especially when dealing with large schemas.
- @lileeyao added --exclude-columns and the --jdbc_options features
- @lileeyao added --jdbc_options
- can now blacklist entire databases
- new kafka key format available, using a JSON array instead of an object
- bugfix: unsigned integer columns were captured incorrectly.  1.1 will
  recapture the schema and attempt to correct the error.

### [v1.1.0-pre4](https://github.com/zendesk/maxwell/releases/tag/v1.1.0-pre4): 
_2016-05-06_

- Eddie McLean gives some helpful patches around bootstrapping
- Bugfixes for the patch-up-the-schema code around unsigned ints

### [v1.1.0-pre3](https://github.com/zendesk/maxwell/releases/tag/v1.1.0-pre3): 
_2016-05-05_

- forgot to include some updates that back-patch unsigned column
  problems

### [v1.1.0-pre2](https://github.com/zendesk/maxwell/releases/tag/v1.1.0-pre2): 
_2016-05-04_

- fix performance issues when capturing schema in AWS Aurora
- fix a bug in capturing unsigned integer columns

### [v1.0.1](https://github.com/zendesk/maxwell/releases/tag/v1.0.1): 
_2016-04-12_

- fixes a parsing bug with `CURRENT_TIMESTAMP()`

### [v1.0.0](https://github.com/zendesk/maxwell/releases/tag/v1.0.0): Maxwell learns to speak
_2016-03-11_

Since v0.17.0, Maxwell has gotten:
- bootstrapping support
- blacklisting for tables
- flexible kafka partitioning
- replication heartbeats
- GEOMETRY columns
- a whole lotta lotta bugfixes

and I, Osheroff, think the damn thing is stable enough for a 1.0.  So
there.

### [v1.0.0-RC3](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-RC3): 
_2016-03-08_

pull in support for replication heartbeats.  helps in the flakier
network environs.

### [v1.0.0-RC2](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-RC2): 
_2016-02-20_

- fixes the way ALTER DATABASE charset= was handled
- adds proper handling of ALTER TABLE CONVERT TO CHARSET

### [v1.0.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-RC1): 
_2016-02-11_

- modifications to the way the bootstrap utility works
- fix a race condition crash bug in bootstrapping
- fix a parser bug

### [v1.0.0-PRE2](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-PRE2): 
_2016-01-30_

1.0.0-PRE2 brings in a lot of changes that got merged while we were
testing out PRE1.  so, hey.
- Configurable names for the `maxwell` schema database (Kristian Kaufman)
- Configurable key (primary key, id, database) into the kafka partition hash function (Kristian Kaufman)
- Configurable Kafka partition hash function (java hashCode, murmur3) (Kristian Kaufman)
- support GEOMETRY columns, output as well-known-text
- add `--blacklist_tables` option to fully ignore excessive schema changes (Nicolas Maquet)
- bootstrap rows now have 'bootstrap-insert' type

### [v1.0.0-PRE1](https://github.com/zendesk/maxwell/releases/tag/v1.0.0-PRE1): 
_2016-01-09_

- Here we have the preview release of @nmaquet's excellent work around
  bootstrapping initial versions of mysql tables.

### [v0.17.0](https://github.com/zendesk/maxwell/releases/tag/v0.17.0): 
_2016-01-07_

v0.17 is a large bugfix release with one new feature.
- FEATURE: allow specifying an alternative mysql schema-storage server and
  replication server
- BUGFIX: properly handle case-sensitivity by aping the behavior of the
  master server.  Fixes #230.
- BUGFIX: parse some forms of CHECK( ... ) statements.  Fixes #203.
- BUGFIX: many more SQL-parser fixes.  We are mostly through some
  thousands of lines of SQL produced by mysql-test.

### [v0.16.2](https://github.com/zendesk/maxwell/releases/tag/v0.16.2): 
_2015-12-16_

This is a large-ish bugfix release.
- Support, with reservations, binlog_row_image=MINIMAL
- parser bug: handle table names that look like floating points
- parser bug: fix for entity names that have '.', '\', etc in them
- handle UPPERCASE encoding names
- support UCS2 (start trying to operate ok on the mysql-test suite)
- use ObjectOutputStream.reset to fix memory leaks when buffering to disk

### [v0.16.1](https://github.com/zendesk/maxwell/releases/tag/v0.16.1): 
_2015-12-11_

This is a bug-fix-roundup release:
- support ALTER DATABASE
- fix a bunch of parse errors: we've started running mysql-test at
  maxwell and are fixing up failures.
- some modifications to the overflow-to-disk logic; we buffer the input
  and output, and we fix a memory leak

### [v0.16.0](https://github.com/zendesk/maxwell/releases/tag/v0.16.0): 
_2015-12-10_

Version 0.16.0 introduces a feature where UPDATE statements will now
show both the new row image and the old values of the fields that
changed.  Thanks @kristiankaufmann

### [v0.15.0](https://github.com/zendesk/maxwell/releases/tag/v0.15.0): 
_2015-12-07_

- fix a parse problem with indices ordered by ASC/DESC

### [v0.15.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.15.0-RC1): 
_2015-12-04_

- large transactions now buffer to disk instead of crushing maxwell.
- support ALGORITHM=[algo], LOCK=[lock] for 5.6 alters

### [v0.14.6](https://github.com/zendesk/maxwell/releases/tag/v0.14.6): 
_2015-11-27_

- fix TIME column support
- fix parsing on millisecond precision column defintions
- fix CREATE SCHEMA parsing

### [v0.14.5](https://github.com/zendesk/maxwell/releases/tag/v0.14.5): 
_2015-11-25_

- handle BOOLEAN columns with true/false defaults

### [v0.14.4](https://github.com/zendesk/maxwell/releases/tag/v0.14.4): 
_2015-11-24_

- fixes parsing of "mysql comments" (`/*! .. */`)
- More performance improvements, another 10% in a tight loop.

### [v0.14.3](https://github.com/zendesk/maxwell/releases/tag/v0.14.3): 
_2015-11-23_

- fixes a regression in 0.14.2 that creates duplicate copies of the "mysql" database in the schema.

### [v0.14.2](https://github.com/zendesk/maxwell/releases/tag/v0.14.2): 
_2015-11-20_

- capture the mysql database along with the rest of the schema.  Eliding it was a bad premature optimization that led to crashes when tables in the mysql database changed. 

### [v0.14.1](https://github.com/zendesk/maxwell/releases/tag/v0.14.1): 
_2015-11-17_

- fixes a parser bug around named PRIMARY KEYs.

### [v0.14.0](https://github.com/zendesk/maxwell/releases/tag/v0.14.0): 
_2015-11-03_

This release introduces row filters, allowing you to include or exclude tables from maxwell's output based on names or regular expressions.  

### [v0.13.1](https://github.com/zendesk/maxwell/releases/tag/v0.13.1): 
_2015-10-29_

v0.13.1 is a bug fix of v0.13.0 -- fixes a bug where long rows were truncated. 

v0.13.0 contains:
- Big performance boost for maxwell: 75% faster in some benchmarks
- @davidsheldon contributed some nice bug fixes around `CREATE TABLE ... IF NOT EXISTS`, which were previously generating new, bogus copies of the schema.
- we now include a "scavenger thread" that will lazily clean out old, deleted schemas.

### [v0.13.0](https://github.com/zendesk/maxwell/releases/tag/v0.13.0): 
_2015-10-29_

Lucky release number 13 brings some reasonably big changes:
- Big performance boost for maxwell: 75% faster in some benchmarks
- @davidsheldon contributed some nice bug fixes around `CREATE TABLE ... IF NOT EXISTS`, which were previously generating new, bogus copies of the schema.
- we now include a "scavenger thread" that will lazily clean out old, deleted schemas.

_This release has a pretty bad bug.  do not use._

### [v0.12.0](https://github.com/zendesk/maxwell/releases/tag/v0.12.0): 
_2015-10-16_

- add support for BIT columns.  

### [v0.11.4](https://github.com/zendesk/maxwell/releases/tag/v0.11.4): 
_2015-09-30_

this is another bugfix release that fixes a problem where the replication thread can die in the middle of processing a transaction event.  I really need to fix this at a lower level, ie the open-replicator level.

### [v0.11.3](https://github.com/zendesk/maxwell/releases/tag/v0.11.3): 
_2015-09-29_

this is a bugfix release:
- fix problems with table creation options inside alter statements ( `ALTER TABLE foo auto_increment=10` )
- fix a host of shutdown-procedure bugs

the test suite should also be way more reliable, not like you care.

### [v0.11.2](https://github.com/zendesk/maxwell/releases/tag/v0.11.2): 
_2015-09-18_

This is a bugfix release.  It includes:
- soft deletions of maxwell.schemas to fix A->B->A master swapping without creating intense replication delay
- detect and fail early if we see `binlog_row_image=minimal`
- kill off maxwell if the position thread dies
- fix a bug where maxwell could pick up a copy of schema from a different server_id (curse you operator precedence!)

### [v0.11.1](https://github.com/zendesk/maxwell/releases/tag/v0.11.1): 
_2015-09-16_

- maxwell gets a very minimal pass at detecting when a master has changed, in which it will kill off schemas and positions from a server_id that no longer is valid.  this should prevent the worst of cases.

### [v0.11.0](https://github.com/zendesk/maxwell/releases/tag/v0.11.0): 
_2015-09-15_

This release of Maxwell preserves transaction information in the kafka stream by adding a `xid` key in the JSON object, as well as a `commit` key for the final row inside the transaction.

It also contains a bugfix around server_id handling.

### [v0.10.1](https://github.com/zendesk/maxwell/releases/tag/v0.10.1): 
_2015-09-11_

- proper support for BLOB, BINARY, VARBINARY columns (base 64 encoded)
- fix a problem with the SQL parser where specifying encoding or collation in a string column in the wrong order would crash
- make table option parsing more lenient

### [v0.11.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-RC1): 
_2015-09-09_

- merge master fixes

### [v0.10.0](https://github.com/zendesk/maxwell/releases/tag/v0.10.0): 
_2015-09-09_

- Mysql 5.6 checksum support!
- some more bugfixes with the SQL parser 

### [v0.11.0-PRE4](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE4): 
_2015-09-09_

- bugfix on v0.11.0-PRE3

### [v0.11.0-PRE3](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE3): 
_2015-09-08_

- handle SAVEPOINT within transactions
- downgrade unhandled SQL to a warning

### [v0.11.0-PRE2](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE2): 
_2015-09-03_

- fixes for myISAM "transactions"

### [v0.11.0-PRE1](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE1): 
_2015-09-02_

- fix a server_id bug (was always 1 in maxwell.schemas)
- JSON output now includes transaction IDs

### [v0.10.0-RC4](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC4): 
_2015-08-31_

- deal with BINARY flag in string column creation.

### [v0.9.5](https://github.com/zendesk/maxwell/releases/tag/v0.9.5): 
_2015-08-31_

- handle the BINARY flag in column creation

### [v0.10.0-RC3](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC3): 
_2015-08-27_

- handle "TRUNCATE [TABLE_NAME]" statements

### [v0.10.0-RC2](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC2): 
_2015-08-26_

- fixes a bug with checksum processing.

### [v0.10.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC1): 
_2015-08-04_

- upgrade to open-replicator 1.3.0-RC1, which brings binlog checksum (and thus easy 5.6.1) support to maxwell.

### [v0.9.4](https://github.com/zendesk/maxwell/releases/tag/v0.9.4): 
_2015-07-27_

- allow a configurable number (including unlimited) of schemas to be stored

### [v0.9.3](https://github.com/zendesk/maxwell/releases/tag/v0.9.3): 
_2015-07-14_

- bump open-replicator to 1.2.3, which allows processing of single rows greater than 2^24 bytes

### [v0.9.2](https://github.com/zendesk/maxwell/releases/tag/v0.9.2): 
_2015-07-10_

- bump open-replicator buffer to 50mb by default
- log to STDERR, not STDOUT 
- `--output_file` option for file producer

### [v0.9.1](https://github.com/zendesk/maxwell/releases/tag/v0.9.1): 
_2015-06-22_

- Maxwell is now aware that column names are case-insenstive
- fix a nasty bug in which maxwell would store the wrong position after it lost its connection to the master.

### [v0.9.0](https://github.com/zendesk/maxwell/releases/tag/v0.9.0): 
_2015-06-18_

Also, vanchi is so paranoid he's worried immediately about this. 

- mysql 5.6 support (without checksum support, yet)
- fix a bunch of miscellaneous bugs @akshayi1 found (REAL, BOOL, BOOLEAN types, TRUNCATE TABLE)

### [v0.8.1](https://github.com/zendesk/maxwell/releases/tag/v0.8.1): 
_2015-06-16_

- minor bugfix release around mysql connections going away.

### [v0.8.0](https://github.com/zendesk/maxwell/releases/tag/v0.8.0): 
_2015-06-09_

- add "ts" field to row output
- add --config option for passing a different config file
- support int1, int2, int4, int8 columns

### [v0.7.2](https://github.com/zendesk/maxwell/releases/tag/v0.7.2): 
_2015-05-29_

- handle inline sql comments
- ignore more user management SQL

### [v0.7.1](https://github.com/zendesk/maxwell/releases/tag/v0.7.1): 
_2015-05-15_

- only keep 5 most recent schemas

### [v0.7.0](https://github.com/zendesk/maxwell/releases/tag/v0.7.0): 
_2015-04-28_

- handle CURRENT_TIMESTAMP parsing properly
- better binlog position sync behavior

### [v0.6.3](https://github.com/zendesk/maxwell/releases/tag/v0.6.3): 
_2015-04-13_

- better blacklist for CREATE TRIGGER

### [v0.6.2](https://github.com/zendesk/maxwell/releases/tag/v0.6.2): 
_2015-04-13_

- maxwell now ignores SAVEPOINT statements.

### [v0.6.1](https://github.com/zendesk/maxwell/releases/tag/v0.6.1): 
_2015-04-13_

- fixes a bug with parsing length-limited indexes.

### [v0.6.0](https://github.com/zendesk/maxwell/releases/tag/v0.6.0): 
_2015-04-09_

Version 0.6.0 has Maxwell outputting a JSON kafka key, so that one can use Kafka's neat "store the last copy of a key" retention policy.  It also fixes a couple of bugs in the query parsing path.

### [v0.5.0](https://github.com/zendesk/maxwell/releases/tag/v0.5.0): 
_2015-04-06_

- maxwell now captures primary keys on tables.  We'll use this to form kafka key names later.
- maxwell now outputs to a single topic, hashing the data by database name to keep a database's updates in order.

### [v0.4.0](https://github.com/zendesk/maxwell/releases/tag/v0.4.0): 
_2015-03-25_

v0.4.0 fixes some bugs with long-lived mysql connections by adding connection pooling support.

### [v0.3.0](https://github.com/zendesk/maxwell/releases/tag/v0.3.0): 
_2015-03-24_

This version fixes a fairly nasty bug in which the binlog-position flush thread was sharing a connection with the rest of the system, leading to crashes. 

It also enables kafka gzip compression by default.

### [v0.2.2](https://github.com/zendesk/maxwell/releases/tag/v0.2.2): 
_2015-03-22_

Version 0.2.2 sets up the LANG environment variable, which fixes a bug in utf-8 handling. 

### [v0.2.1](https://github.com/zendesk/maxwell/releases/tag/v0.2.1): 
_2015-03-21_

version 0.2.1 makes Maxwell ignore CREATE INDEX ddl statements and others.

### [v0.2.0](https://github.com/zendesk/maxwell/releases/tag/v0.2.0): 
_2015-03-18_

This release gets Maxwell storing the last-written binlog position inside the mysql master itself. 

### [v0.1.4](https://github.com/zendesk/maxwell/releases/tag/v0.1.4): 
_2015-03-09_

support --position_file param

### [v0.1.3](https://github.com/zendesk/maxwell/releases/tag/v0.1.3): 
_2015-03-09_

Adds kafka command line options.

### [v0.1.1](https://github.com/zendesk/maxwell/releases/tag/v0.1.1): 
_2015-03-06_

v0.1.1, a small bugfix release. 

### [v0.1](https://github.com/zendesk/maxwell/releases/tag/v0.1): 
_2015-03-04_

This is the first possible release of Maxwell that might work.  It includes some exceedingly basic kafka support, and JSON output of binlog deltas.

