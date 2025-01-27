package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * The BootstrapTask class represents a task for bootstrapping a database table and provides methods for creating,
 * manipulating, and matching tasks.
 */
public class BootstrapTask {
	public String database;
	public String table;
	public String whereClause;
	public Long id;
	public BinlogPosition startPosition;
	public boolean complete;
	public Timestamp startedAt;
	public Timestamp completedAt;
	public String comment;

	public volatile boolean abort;


	public String logString() {
		String s = String.format("#%d %s.%s", id, database, table);
		if ( whereClause != null )
			s += " WHERE " + whereClause;
		return s;
	}

	static BootstrapTask valueOf(ResultSet rs) throws SQLException {
		BootstrapTask task = new BootstrapTask();
		task.id = rs.getLong("id");
		task.database = rs.getString("database_name");
		task.table = rs.getString("table_name");
		task.whereClause = rs.getString("where_clause");
		task.startPosition = null;
		task.complete = rs.getBoolean("is_complete");
		task.completedAt = rs.getTimestamp("completed_at");
		task.startedAt = rs.getTimestamp("started_at");
		task.comment = rs.getString("comment");
		return task;
	}

	public static BootstrapTask valueOf(RowMap row) {
		BootstrapTask t = new BootstrapTask();
		t.database = (String) row.getData("database_name");
		t.table = (String) row.getData("table_name");
		t.whereClause = (String) row.getData("where_clause");
		t.id = (Long) row.getData("id");

		String binlogFile = (String) row.getData("binlog_file");
		Long binlogOffset = (Long) row.getData("binlog_position");

		t.startPosition = BinlogPosition.at(binlogOffset, binlogFile);
		return t;
	}

	public boolean matches(RowMap row) {
		return database.equalsIgnoreCase(row.getDatabase())
			&& table.equalsIgnoreCase(row.getTable());
	}
}
