package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.schema.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.Schema;

public class DatabaseDrop extends SchemaChange {
	public String database;
	public boolean ifExists;

	public DatabaseDrop(String database, boolean ifExists) {
		this.database = database;
		this.ifExists = ifExists;
	}

	@Override
	public ResolvedDatabaseDrop resolve(Schema schema) throws InvalidSchemaError {
		if ( ifExists && !schema.hasDatabase(database) )
			return null;

		return new ResolvedDatabaseDrop(this.database);
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isDatabaseBlacklisted(database);
		}
	}

}
