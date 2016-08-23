package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class ResolvedDatabaseDrop extends ResolvedSchemaChange {
	public String database;
	public Database def;

	public ResolvedDatabaseDrop() {}
	public ResolvedDatabaseDrop(String database, Database def) {
		this.database = database;
		this.def = def;
	}

	@Override
	public Schema apply(Schema originalSchema) throws InvalidSchemaError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabaseOrThrow(database);
		newSchema.getDatabases().remove(d);
		return newSchema;
	}
}
