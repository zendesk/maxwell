package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class ResolvedDatabaseDrop extends ResolvedSchemaChange {
	public String database;

	public ResolvedDatabaseDrop() { }
	public ResolvedDatabaseDrop(String database) {
		this.database = database;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		if ( !newSchema.hasDatabase(database) )
			throw new SchemaSyncError("Can't drop missing database: " + database);

		newSchema.getDatabases().remove(newSchema.findDatabase(database));
		return newSchema;
	}
}
