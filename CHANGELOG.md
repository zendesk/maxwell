# Maxwell changelog

## [v0.16.0](https://github.com/zendesk/maxwell/releases/tag/v0.16.0): "Kristian Kaufmann's version"


Version 0.16.0 introduces a feature where UPDATE statements will now
show both the new row image and the old values of the fields that
changed.  Thanks @kristiankaufmann


## [v0.15.0](https://github.com/zendesk/maxwell/releases/tag/v0.15.0): "the littlest little city"

- fix a parse problem with indices ordered by ASC/DESC


## [v0.15.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.15.0-RC1): "it's later than you think"


- large transactions now buffer to disk instead of crushing maxwell.
- support ALGORITHM=[algo], LOCK=[lock] for 5.6 alters


## [v0.14.6](https://github.com/zendesk/maxwell/releases/tag/v0.14.6): "It's about being American.  Sort of."


- fix TIME column support
- fix parsing on millisecond precision column defintions
- fix CREATE SCHEMA parsing


## [v0.14.5](https://github.com/zendesk/maxwell/releases/tag/v0.14.5): "false is the new true"


- handle BOOLEAN columns with true/false defaults


## [v0.14.4](https://github.com/zendesk/maxwell/releases/tag/v0.14.4): "You'd think we'd be at 1.0 by now, wouldn't you?"

- fixes parsing of "mysql comments" (`/*! .. */`)
- More performance improvements, another 10% in a tight loop.


## [v0.14.3](https://github.com/zendesk/maxwell/releases/tag/v0.14.3): "Peanuts.  My girlfriend thinks about peanuts."

- fixes a regression in 0.14.2 that creates duplicate copies of the "mysql" database in the schema.


## [v0.14.2](https://github.com/zendesk/maxwell/releases/tag/v0.14.2): "Maxwell Sandvik 88"

- capture the mysql database along with the rest of the schema.  Eliding it was a bad premature optimization that led to crashes when tables in the mysql database changed. 


## [v0.14.1](https://github.com/zendesk/maxwell/releases/tag/v0.14.1): "be liberal in what you accept.  Even if nonsensical."

- fixes a parser bug around named PRIMARY KEYs.

## [v0.14.0](https://github.com/zendesk/maxwell/releases/tag/v0.14.0): "the slow but inevitable slide"

This release introduces row filters, allowing you to include or exclude tables from maxwell's output based on names or regular expressions.  


## [v0.13.1](https://github.com/zendesk/maxwell/releases/tag/v0.13.1): "well that was somewhat expected"

v0.13.1 is a bug fix of v0.13.0 -- fixes a bug where long rows were truncated. 

v0.13.0 contains:

- Big performance boost for maxwell: 75% faster in some benchmarks
- @davidsheldon contributed some nice bug fixes around `CREATE TABLE ... IF NOT EXISTS`, which were previously generating new, bogus copies of the schema.
- we now include a "scavenger thread" that will lazily clean out old, deleted schemas.

## [v0.13.0](https://github.com/zendesk/maxwell/releases/tag/v0.13.0): "Malkovich Malkovich Malkovich Sheldon?"

Lucky release number 13 brings some reasonably big changes:

- Big performance boost for maxwell: 75% faster in some benchmarks
- @davidsheldon contributed some nice bug fixes around `CREATE TABLE ... IF NOT EXISTS`, which were previously generating new, bogus copies of the schema.
- we now include a "scavenger thread" that will lazily clean out old, deleted schemas.

*This release has a pretty bad bug.  do not use.*

## [v0.12.0](https://github.com/zendesk/maxwell/releases/tag/v0.12.0): "what do I call them?  Slippers?  Why, are you jealous?"

- add support for BIT columns.  

## [v0.11.4](https://github.com/zendesk/maxwell/releases/tag/v0.11.4): "13 steps"

this is another bugfix release that fixes a problem where the replication thread can die in the middle of processing a transaction event.  I really need to fix this at a lower level, ie the open-replicator level.


## [v0.11.3](https://github.com/zendesk/maxwell/releases/tag/v0.11.3): ".. and the other half is to take the bugs out"

this is a bugfix release:
- fix problems with table creation options inside alter statements ( `ALTER TABLE foo auto_increment=10` )
- fix a host of shutdown-procedure bugs

the test suite should also be way more reliable, not like you care.

## [v0.11.2](https://github.com/zendesk/maxwell/releases/tag/v0.11.2): "savage acts of unprovoked violence are bad"

This is a bugfix release.  It includes:
- soft deletions of maxwell.schemas to fix A->B->A master swapping without creating intense replication delay
- detect and fail early if we see `binlog_row_image=minimal`
- kill off maxwell if the position thread dies
- fix a bug where maxwell could pick up a copy of schema from a different server_id (curse you operator precedence!)

## [v0.11.1](https://github.com/zendesk/maxwell/releases/tag/v0.11.1): "dog snoring loudly"

- maxwell gets a very minimal pass at detecting when a master has changed, in which it will kill off schemas and positions from a server_id that no longer is valid.  this should prevent the worst of cases.

## [v0.11.0](https://github.com/zendesk/maxwell/releases/tag/v0.11.0): "cat waving gently"

This release of Maxwell preserves transaction information in the kafka stream by adding a `xid` key in the JSON object, as well as a `commit` key for the final row inside the transaction.

It also contains a bugfix around server_id handling.

## [v0.10.1](https://github.com/zendesk/maxwell/releases/tag/v0.10.1): "all 64 of your bases belong to... shut up, internet parrot."

- proper support for BLOB, BINARY, VARBINARY columns (base 64 encoded)
- fix a problem with the SQL parser where specifying encoding or collation in a string column in the wrong order would crash
- make table option parsing more lenient

## [v0.11.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-RC1): "goin' faster than a rollercoaster"

- merge master fixes

## [v0.10.0](https://github.com/zendesk/maxwell/releases/tag/v0.10.0): "The first word is French"

- Mysql 5.6 checksum support!
- some more bugfixes with the SQL parser 


## [v0.11.0-PRE4](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE4): "except for that other thing"

- bugfix on v0.11.0-PRE3

## [v0.11.0-PRE3](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE3): "nothing like a good night's sleep"

- handle SAVEPOINT within transactions
- downgrade unhandled SQL to a warning

## [v0.11.0-PRE2](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE2): "you really need to name a *PRE* release something cutesy?"

- fixes for myISAM "transactions"

## [v0.11.0-PRE1](https://github.com/zendesk/maxwell/releases/tag/v0.11.0-PRE1): "A slow traffic jam towards the void"

- fix a server_id bug (was always 1 in maxwell.schemas)
- JSON output now includes transaction IDs

## [v0.10.0-RC4](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC4): "Inspiring confidence"

- deal with BINARY flag in string column creation.

## [v0.9.5](https://github.com/zendesk/maxwell/releases/tag/v0.9.5): "Long story short, that's why I'm late"

- handle the BINARY flag in column creation

## [v0.10.0-RC3](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC3): "Except for that one thing"

- handle "TRUNCATE [TABLE_NAME]" statements

## [v0.10.0-RC2](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC2): "RC2 is always a good sign."

- fixes a bug with checksum processing.


## [v0.10.0-RC1](https://github.com/zendesk/maxwell/releases/tag/v0.10.0-RC1): "verify all the things"

- upgrade to open-replicator 1.3.0-RC1, which brings binlog checksum (and thus easy 5.6.1) support to maxwell.

## [v0.9.4](https://github.com/zendesk/maxwell/releases/tag/v0.9.4): "we've been here before"

- allow a configurable number (including unlimited) of schemas to be stored

## [v0.9.3](https://github.com/zendesk/maxwell/releases/tag/v0.9.3): "some days it's just better to stay in bed"

- bump open-replicator to 1.2.3, which allows processing of single rows greater than 2^24 bytes

## [v0.9.2](https://github.com/zendesk/maxwell/releases/tag/v0.9.2): "Cat's tongue"

- bump open-replicator buffer to 50mb by default
- log to STDERR, not STDOUT 
- `--output_file` option for file producer

## [v0.9.1](https://github.com/zendesk/maxwell/releases/tag/v0.9.1): "bugs, bugs, bugs, lies, statistics"

- Maxwell is now aware that column names are case-insenstive
- fix a nasty bug in which maxwell would store the wrong position after it lost its connection to the master.

## [v0.9.0](https://github.com/zendesk/maxwell/releases/tag/v0.9.0): Vanchi says "eat"

Also, vanchi is so paranoid he's worried immediately about this. 

- mysql 5.6 support (without checksum support, yet)
- fix a bunch of miscellaneous bugs @akshayi1 found (REAL, BOOL, BOOLEAN types, TRUNCATE TABLE)

## [v0.8.1](https://github.com/zendesk/maxwell/releases/tag/v0.8.1): "Pascal says Bonjour"

- minor bugfix release around mysql connections going away.

## [v0.8.0](https://github.com/zendesk/maxwell/releases/tag/v0.8.0): the cat never shuts up

- add "ts" field to row output
- add --config option for passing a different config file
- support int1, int2, int4, int8 columns


## [v0.7.2](https://github.com/zendesk/maxwell/releases/tag/v0.7.2): "all the sql ladies"

- handle inline sql comments
- ignore more user management SQL

## [v0.7.1](https://github.com/zendesk/maxwell/releases/tag/v0.7.1): "not hoarders"

- only keep 5 most recent schemas

## [v0.7.0](https://github.com/zendesk/maxwell/releases/tag/v0.7.0): 0.7.0, "alameda"

- handle CURRENT_TIMESTAMP parsing properly
- better binlog position sync behavior

## [v0.6.3](https://github.com/zendesk/maxwell/releases/tag/v0.6.3): 0.6.3

- better blacklist for CREATE TRIGGER

## [v0.6.2](https://github.com/zendesk/maxwell/releases/tag/v0.6.2): v0.6.2

- maxwell now ignores SAVEPOINT statements.

## [v0.6.1](https://github.com/zendesk/maxwell/releases/tag/v0.6.1): v0.6.1

- fixes a bug with parsing length-limited indexes.

## [v0.6.0](https://github.com/zendesk/maxwell/releases/tag/v0.6.0): kafkakafkakafa

Version 0.6.0 has Maxwell outputting a JSON kafka key, so that one can use Kafka's neat "store the last copy of a key" retention policy.  It also fixes a couple of bugs in the query parsing path.

## [v0.5.0](https://github.com/zendesk/maxwell/releases/tag/v0.5.0): 0.5.0 -- "People who put commas in column names deserve undefined behavior"

- maxwell now captures primary keys on tables.  We'll use this to form kafka key names later.
- maxwell now outputs to a single topic, hashing the data by database name to keep a database's updates in order.

## [v0.4.0](https://github.com/zendesk/maxwell/releases/tag/v0.4.0): 0.4.0, "unboxed cat"

v0.4.0 fixes some bugs with long-lived mysql connections by adding connection pooling support.

## [v0.3.0](https://github.com/zendesk/maxwell/releases/tag/v0.3.0): 0.3.0

This version fixes a fairly nasty bug in which the binlog-position flush thread was sharing a connection with the rest of the system, leading to crashes. 

It also enables kafka gzip compression by default.

## [v0.2.2](https://github.com/zendesk/maxwell/releases/tag/v0.2.2): 0.2.2

Version 0.2.2 sets up the LANG environment variable, which fixes a bug in utf-8 handling. 

## [v0.2.1](https://github.com/zendesk/maxwell/releases/tag/v0.2.1): v0.2.1

version 0.2.1 makes Maxwell ignore CREATE INDEX ddl statements and others.

## [v0.2.0](https://github.com/zendesk/maxwell/releases/tag/v0.2.0): 0.2.0

This release gets Maxwell storing the last-written binlog position inside the mysql master itself. 

## [v0.1.4](https://github.com/zendesk/maxwell/releases/tag/v0.1.4): 0.1.4

support --position_file param

## [v0.1.3](https://github.com/zendesk/maxwell/releases/tag/v0.1.3): 0.1.3

Adds kafka command line options.

## [v0.1.1](https://github.com/zendesk/maxwell/releases/tag/v0.1.1): 0.1.1

v0.1.1, a small bugfix release. 

## [v0.1](https://github.com/zendesk/maxwell/releases/tag/v0.1): 0.1

This is the first possible release of Maxwell that might work.  It includes some exceedingly basic kafka support, and JSON output of binlog deltas.


