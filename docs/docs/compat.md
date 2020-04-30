### Requirements:
***
- JRE 7 or above
- mysql 5.1, 5.5, 5.6, 5.7, 8
- kafka 0.8.2 or greater

### binlog_row_image=MINIMAL
***
As of 0.16.2, Maxwell supports binlog_row_image=MINIMAL, but it may not be what you want.  It will differ
from normal Maxwell operation in that:

- INSERT statements will no longer output a column's default value
- UPDATE statements will be incomplete; Maxwell outputs as much of the row as given in the binlogs,
  but `data` will only include what is needed to perform the update (generally, id columns and changed columns).
  The `old` section may or may not be included, depending on the nature of the update.
- DELETE statements will be incomplete; generally they will only include the primary key.

### Master recovery
***

As of 1.2.0, maxwell includes experimental support for master position recovery.  It works like this:

- maxwell writes heartbeats into the binlogs (via the `positions` table)
- maxwell reads its own heartbeats, using them as a secondary position guide
- if maxwell boots and can't find its position matching the `server_id` it's
  connecting to, it will look for a row in `maxwell.positions` from a different
  server_id.
- if it finds that row, it will scan backwards in the binary logs of the new
  master until it finds that heartbeat.

Notes:

- master recovery is not compatible with separate schema-store hosts and
  replication-hosts, due to the heartbeat mechanism.
- this code should be considered alpha-quality.
- on highly active servers, as much as 1 second of data may be duplicated.
- master recovery is not available in GTID-mode.

### MySQL binlog connector
***

As of 1.11.0, maxwell uses [shyiko/mysql-binlog-connector-java][] as its underlying
replication library (previously it was opt-in via `--binlog_connector`). This is
largely compatible with the previous OpenReplicator implementation, but there are some differences:

 - TIMESTAMP columns are always treated as UTC, regardless of your timezone. See
   [issue #681][issue-681] for more details.

[shyiko/mysql-binlog-connector-java]: https://github.com/shyiko/mysql-binlog-connector-java
[issue-681]: https://github.com/zendesk/maxwell/issues/681
