package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.*;

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
	public ResolvedTableDrop resolve(Schema schema) throws InvalidSchemaError {
		//Brady Auen: added Table so we could successfully drop table's columns
		Table def = null;

		if ( ifExists ) {
			Database d = schema.findDatabase(this.database);
			if ( d == null || !d.hasTable(table) )
				return null;
			else
				def = d.findTableOrThrow(this.table);
		}

		return new ResolvedTableDrop(this.database, table, def);
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
