### Schema storage
***
This document describes Maxwell's internal schema storage mechanism. Most users won't need to know how this works, but it can be useful in the rare case where something goes wrong.

MySQL binlogs give maxwell access to the raw bytes for each update that happens. In order to generate useful output, Maxwell needs to know the data type of every column, so that it can interpret the bytes as a number, string, boolean, etc.

There is a set of "base schema" tables in the maxwell database - `tables`, `columns`, `databases`. This is where we capture the initial schema, the first time Maxwell is run.

As time progresses, maxwell will see any modifications you make to the schema (these are part of the binlog). As each change occurs, Maxwell will generate an internal representation of what changed. Maxwell stores these diffs in the `schemas` table - each diff contains the following information to place it in the timeline of your database:

 - `binlog_file`, `binlog_position` (or `gtid_set`): the exact point in the binlog stream where the schema change occurred
 - `deltas`: the internal representation of the schema change
 - `base_schema_id`: the previous schema that this delta applies to
 - `last_heartbeat_read`: the most recent maxwell heartbeat seen in the binlog prior to this change
 - `server_id`: the server which this schema applies to

This information creates a concrete timeline of the history of your schema for a given server. Given any binlog file+position (or gtid) and a last_heartbeat value, the current schema can be found by finding the "most recent" schema for this server_id, then following the chain of `base_schema_id` until it terminates (in a `null`, which means we've reached the initial captured schema).

"Most recent" should be fairly intuitive - firstly we sort by `last_heartbeat_read`, then by `binlog_file`, then `binlog_position`. We limit the search to values "before" the binlog position we're searching for, because we don't want to use a schema corresponding to a change that is "in the future" - i.e. further ahead in the binlog than our current position.

### Master failover
***
Schemas are important for master failover. When Maxwell detects that it is talking to a new `server_id` (one that differs from its stored `position`), it attempts a master failover (if enabled). It searches backwards in the new servers binlog files, looking for an update to `maxwell.heartbeats` corresponding to the timestamp stored in its position table.

Once it finds this (unique) update, it knows the binlog location for both the old and new master which correspond to the exact same event.

Using this information, maxwell creates a merge point - it finds the active schema for the old master's stored position, and then creates a new schema entry with an empty delta, the new `server_id`, and a base_schema_id of the previous schema. In this way, Maxwell is able to create a chain of schema updates even across different servers with different sets of binlogs.
