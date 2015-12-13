<div class="content-title">Maxwell Compatibility</div>

### Requirements:

- JRE 7 or above
- mysql 5.1, 5.5, 5.6
- kafka 0.8.2 or greater

### Unsupported

- Mysql 5.7 is untested with Maxwell, and in particular GTID replication is unsupported as of yet.

### binlog_row_image=MINIMAL

As of 0.17.0, Maxwell supports binlog_row_image=MINIMAL, but it may not be what you want.  It will differ
from normal Maxwell operation in that:

- INSERT statements will no longer output a column's default value
- UPDATE statements will be incomplete; Maxwell outputs as much of the row as given in the binlogs,
  but `data` will only include what is needed to perform the update (generally, id columns and changed columns).
  The `old` section may or may not be included, depending on the nature of the update.
- DELETE statements will be incomplete; generally they will only include the primary key.

### Master recovery

Currently Maxwell is not very smart about master recovery or detecting a promoted slave; if it determines
that the server_id has changed between runs, Maxwell will simply delete its old schema cache and binlog position
and start again.  We plan on improving master recovery in future releases.

If you know the starting position of your new master, you can start the new Maxwell process with the
`--init_position` flag, which will ensure that no gap appears in a master failover.


