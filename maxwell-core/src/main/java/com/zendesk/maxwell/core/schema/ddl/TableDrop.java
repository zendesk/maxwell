package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.core.schema.Database;
import com.zendesk.maxwell.core.schema.Schema;

public class TableDrop extends SchemaChange {
	public String database;
	final String table;
	final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

	@Override
	public ResolvedTableDrop resolve(Schema schema) {
		if ( ifExists ) {
			Database d = schema.findDatabase(this.database);
			if ( d == null || !d.hasTable(table) )
				return null;
		}

		return new ResolvedTableDrop(database, table);
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.database, this.table);
		}
	}

}
