/*
 * your javascript filter should define at least
 * one of (process_row, process_heartbeat, process_ddl).
 *
 * `process_row` is called post-filter, for all unfiltered data rows
 *
 * process_row must take one parameter, an instance of `WrappedRowMap`.  WrappedRowMap's javascript api looks like:
 *
 * .suppress() -> call this to remove row from output
 * .kafka_topic = "topic" -> override the default kafka topic for this row
 * .data -> hash containing row data.  May be modified.
 * .old_data -> hash containing old data
 * .extra_attributes -> if you set values in this hash, they will be output at the top level of the final JSON
 * .type -> "insert" | "update" | "delete"
 * .table -> table name
 * .database -> database name
 * .position -> binlog position of event as string
 * .xid -> transaction XID
 * .server_id -> server id
 * .thread_id -> client thread_id
 * .timestamp -> row timestamp
 *
 */


function process_row(row) {
	if ( row.database == "shard_1" ) {
		if ( row.table == "fo" ) {
			row.suppress();
		} else if ( row.table == "sharded" ) {
			if ( row.type == "insert" ) {
				row.data.put("password", "XXXXXXXX");
			}
		} else if ( row.table == "other" ) {
			row.kafka_topic = "other_kafka_topic";
		}
	}
}

/*
 * `process_ddl` must take one parameter, an instance of `WrappedDDLMap`.  WrappedDDLMap responds to:
 * .suppress() -> call this to remove row from output
 * .kafka_topic = "topic" -> override the default kafka topic for this row
 * .extra_attributes -> if you set values in this hash, they will be output at the top level of the final JSON
 * .type -> DDL-TYPE
 * .table -> table name
 * .database -> database name
 * .position -> binlog position of event as string
 * .timestamp -> row timestamp in utc seconds
 * .change -> hash-map representing the DDL change.  different for each type of DDL.
 */

function process_ddl(ddl) {
	logger.info(ddl.change);
	logger.info(ddl.table);
	logger.info(ddl.database);

	ddl.extra_attributes['hello'] = 'world';
	if ( ddl.table == "foo" )
		ddl.suppress();
}


/*
 * `process_heartbeat` must take one parameter, an instance of `WrappedHeartbeatMap`.  WrappedHeartbeatMap responds to:
 * .position -> binlog position of event as string
 * .timestamp -> row timestamp in utc seconds
 * .heartbeat -> timestamp of when heartbeat was sent, in utc milliseconds
 */

function process_heartbeat(heartbeat) {
	logger.info(heartbeat.position);
	logger.info(heartbeat.timestamp);
	logger.info(heartbeat.heartbeat);
}

