package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class ResolvedDatabaseDrop extends ResolvedSchemaChange {
	public String database;

	public ResolvedDatabaseDrop() { }
	public ResolvedDatabaseDrop(String database) {
		this.database = database;
	}

	@Override
	public Schema apply(Schema originalSchema) throws InvalidSchemaError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabaseOrThrow(database);
		newSchema.getDatabases().remove(d);
		return newSchema;
	}
}
