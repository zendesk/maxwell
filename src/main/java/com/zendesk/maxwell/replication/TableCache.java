package com.zendesk.maxwell.replication;

import java.util.HashMap;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class TableCache {
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();
	private final HashMap<Long, String> blacklistedTableCache = new HashMap<>();

	public void processEvent(Schema schema, MaxwellFilter filter, Long tableId, String dbName, String tblName) {
		if ( !tableMapCache.containsKey(tableId) ) {
			if ( filter != null && filter.isTableBlacklisted(dbName, tblName) ) {
				blacklistedTableCache.put(tableId, tblName);
				return;
			}

			Database db = schema.findDatabase(dbName);
			if ( db == null )
				throw new RuntimeException("Couldn't find database " + dbName);
			else {
				Table tbl = db.findTable(tblName);

				if (tbl == null)
					throw new RuntimeException("Couldn't find table " + tblName + " in database " + dbName);
				else
					tableMapCache.put(tableId, tbl);
			}
		}

	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public boolean isTableBlacklisted(Long tableId) {
		return blacklistedTableCache.containsKey(tableId);
	}

	public String getBlacklistedTableName(Long tableId) {
		return blacklistedTableCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
		blacklistedTableCache.clear();
	}
}
