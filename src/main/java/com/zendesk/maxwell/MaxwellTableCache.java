package com.zendesk.maxwell;

import java.util.HashMap;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class MaxwellTableCache {
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();
	// open-replicator keeps a very similar cache, but we can't get access to it.
	public void processEvent(Schema schema, TableMapEvent event) {
		Long tableId = event.getTableId();
		if ( !tableMapCache.containsKey(tableId) ) {
			String dbName = new String(event.getDatabaseName().getValue());
			String tblName = new String(event.getTableName().getValue());
			Database db = schema.findDatabase(dbName);
			if ( db == null )
				throw new RuntimeException("Couldn't find database " + dbName);

			Table tbl = db.findTable(tblName);
			if ( tbl == null )
				throw new RuntimeException("Couldn't find table " + tblName);

			tableMapCache.put(tableId, tbl);
		}
	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
	}
}
