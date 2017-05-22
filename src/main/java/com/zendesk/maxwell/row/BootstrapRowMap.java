package com.zendesk.maxwell.row;

import java.util.ArrayList;

import com.zendesk.maxwell.bootstrap.BootstrapPoller.BootstrapEntry;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.columndef.DateFormatter;

public class BootstrapRowMap extends RowMap {
	public BootstrapRowMap(String type, String database, BootstrapEntry entry, Position position) {
		super(type, database, "bootstrap", System.currentTimeMillis() / 1000, new ArrayList<String>(0), position);
		this.putData("id", (long) entry.id);
		this.putData("database_name", entry.database);
		this.putData("table_name", entry.table);
		this.putData("where_clause", entry.where);
		this.putData("is_complete", entry.complete ? 1L : 0L);
		this.putData("inserted_rows", entry.inserted_rows);
		this.putData("total_rows", entry.total_rows);
		this.putData("created_at", entry.created_at == null ? null : DateFormatter.formatDate(entry.created_at));
		this.putData("started_at", entry.started_at == null ? null : DateFormatter.formatDate(entry.started_at));
		this.putData("completed_at", entry.completed_at == null ? null : DateFormatter.formatDate(entry.completed_at));
		this.putData("binlog_file", entry.binlog_file);
		this.putData("binlog_position", (long) entry.binlog_position);

	}
}
