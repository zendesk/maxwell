<div class="content-title">Maxwell Compatibility</div>

### Requirements:

- JRE 7 or above
- mysql 5.1, 5.5, 5.6
- kafka 0.8.2 or greater

### Unsupported

- Mysql 5.7 is untested with Maxwell, and in particular GTID replication is unsupported as of yet.
- `binlog_row_image=MINIMAL` is not supported and will break Maxwell in a variety of amusing ways.

### Master recovery

Currently Maxwell is not very smart about master recovery or detecting a promoted slave; if it determines
that the server_id has changed between runs, Maxwell will simply delete its old schema cache and binlog position
and start again.  We plan on improving this situation in 0.12.


