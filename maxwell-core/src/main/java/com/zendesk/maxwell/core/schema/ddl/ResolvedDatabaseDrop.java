package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.schema.Database;
import com.zendesk.maxwell.core.schema.Schema;

public class ResolvedDatabaseDrop extends ResolvedSchemaChange {
	public String database;

	public ResolvedDatabaseDrop() { }
	public ResolvedDatabaseDrop(String database) {
		this.database = database;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(database);
		schema.getDatabases().remove(d);
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return null;
	}
}
