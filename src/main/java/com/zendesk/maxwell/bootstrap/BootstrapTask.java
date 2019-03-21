package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;

import java.sql.Timestamp;

public class BootstrapTask {
	public String database;
	public String table;
	public String whereClause;
	public Long id;
	public BinlogPosition startPosition;
	public boolean complete;
	public Timestamp startedAt;
	public Timestamp completedAt;

	public volatile boolean abort;

	public String logString() {
		String s = String.format("#%d %s.%s", id, database, table);
		if ( whereClause != null )
			s += " WHERE " + whereClause;
		return s;
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
}
