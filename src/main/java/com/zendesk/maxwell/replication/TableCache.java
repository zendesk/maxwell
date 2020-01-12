package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TableCache {
	static final Logger LOGGER = LoggerFactory.getLogger(TableCache.class);
	private final String maxwellDB;

	public TableCache(String maxwellDB) {
		this.maxwellDB = maxwellDB;
	}
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();

	public void processEvent(Schema schema, Filter filter, Long tableId, String dbName, String tblName) {
		if ( !tableMapCache.containsKey(tableId)) {
			if ( filter.isTableBlacklisted(dbName, tblName) ) {
				return;
			}

			Database db = schema.findDatabase(dbName);
			if ( db == null )
				LOGGER.warn("Couldn't find database " + dbName);
			else {
				Table tbl = db.findTable(tblName);

				if (tbl == null)
					LOGGER.warn("Couldn't find table " + tblName + " in database " + dbName);
				else
					tableMapCache.put(tableId, tbl);
			}
		}

	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
	}
}
