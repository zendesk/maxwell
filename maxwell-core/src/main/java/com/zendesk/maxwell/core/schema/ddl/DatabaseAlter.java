package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.api.schema.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.Schema;

public class DatabaseAlter extends SchemaChange {
	public String database;
	public String charset;

	public DatabaseAlter(String database) {
		this.database = database;
	}

	@Override
	public ResolvedDatabaseAlter resolve(Schema s) throws InvalidSchemaError {
		return new ResolvedDatabaseAlter(this.database, this.charset);
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
