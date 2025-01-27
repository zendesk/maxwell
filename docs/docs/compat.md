# Compability

## Requirements
***
- JRE 11 or above
- mysql 5.1, 5.5, 5.6, 5.7, 8
- kafka 0.8.2 or greater (if using kafka)

## Caveats / Notes

###binlog_row_image=MINIMAL
***
Maxwell supports binlog_row_image=MINIMAL, but it may not be what you want.  It will differ
from normal Maxwell operation in that:

- INSERT statements will no longer output a column's default value
- UPDATE statements will be incomplete; Maxwell outputs as much of the row as given in the binlogs,
  but `data` will only include what is needed to perform the update (generally, id columns and changed columns).
  The `old` section may or may not be included, depending on the nature of the update.
- DELETE statements will be incomplete; generally they will only include the primary key.

### Master recovery
***

Maxwell includes support for master position recovery (non-GTID).  It works like this:

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
- GTID is generally a preferred method of master failover.

