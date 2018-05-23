package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.filtering.FilterV2;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.ddl.ResolvedDatabaseAlter;

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
	public boolean isBlacklisted(FilterV2 filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isDatabaseBlacklisted(database);
		}
	}
}
