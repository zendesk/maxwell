# Maxwell's Daemon

This is some information for developers on Maxwell's Deamon.  If you're looking
for user or operational documentation, you should go here instead:

[http://maxwells-daemon.io/](http://maxwells-daemon.io/)

## Getting Started

You'll need:

- jdk, and `javac` in the path
- make

To build maxwell:

```
$ make
```

To run tests:

```
$ make test
```

To run a specific test:

```
$ make test.MaxwellIntegrationTest
```

Maxwell uses some makefile-fu to build and run tests, but if you happen to like
maven and pom.xml, that'll work too.   Just don't add any maven-specific plugins.

## Coding style

Tabs-only, no spaces.  If you break function calls across lines, do it like:
```
  callFoo(
    bar,
    baz
  )
```

## Building documentation

Maxwell uses the excellent [http://www.mkdocs.org/](mkdocs) package with a custom theme for its
use-facing documentation.  To view your changes, you should get mkdocs via pip
or easy_install or whatever, and then do:

```
$ cd docs
$ mkdocs serve
```

which will start a webserver on localhost for you.


## Gizmo tour

Maxwell is designed as a near complete mysql replica; it decodes binlog events,
maintains a view of the current schema (inside of mysql itself) and parses data
definition language (DDL) SQL to keep that schema current schema.

At its core is the
[https://github.com/zendesk/maxwell/blob/master/src/main/java/com/zendesk/maxwell/replication/MaxwellReplicator.java](MaxwellReplicator) class,
which current sits atop a fork of
[http://github.com/zendesk/open-replicator](open-replicator) (a port to shyko's
rewrite is in progress).  `MaxwellReplicator` holds a background thread that
consumes events from a remote mysql server and pushes them, raw, into a queue.
Those events are then combined with the current mysql schema, converted into a
[https://github.com/zendesk/maxwell/blob/master/src/main/java/com/zendesk/maxwell/row/RowMap.java](RowMap),
and handled off to one of a few different producers.  The most advanced of
these is the
[https://github.com/zendesk/maxwell/blob/master/src/main/java/com/zendesk/maxwell/producer/MaxwellKafkaProducer.java](MaxwellKafkaProducer),
which will convert the `RowMap` into JSON before sending it to Kafka.  Once the
message has been acked by Kafka, the binlog position is advanced and stored
back inside mysql.

whew.

## Heartbeats / Master recovery

As of 1.2.0, maxwell includes experimental support for master position
recovery. It works like this:

Normal runtime:

1. maxwell writes heartbeats into the binlogs (via the `maxwell.heartbeats` table)
2. whilst replicating, maxwell will take note of these heartbeats and scribble down the
   position where it finds them.  It stores the `last_heartbeat_read` along with the position
   inside the `maxwell.positions` table.

Recovery:

1. Maxwell boots up and finds itself to be on a server it doesn't recognize (it doesn't
   find a row in `maxwell.positions` from the server_id it's currently connected to.
2. Maxwell checks to see if there's a row in `maxwell.positions` from a different server_id (the old master).
3. Maxwell picks up the `last_heartbeat_read` value from that row.
4. Maxwell "scans backwards" in the binary logs of the new master until it
   finds that heartbeat: If the current binlog position is "new_master.000005:41322",
   Maxwell will start replication at "new_master.000005:4" and scan forward, attempting to find
   its heartbeat value.  If it fails to find it in this file, it will try again at "new_master.000004:4".

