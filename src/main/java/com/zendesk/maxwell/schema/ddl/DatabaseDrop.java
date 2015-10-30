package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseDrop extends SchemaChange {
	public String dbName;
	public boolean ifExists;

	public DatabaseDrop(String dbName, boolean ifExists) {
		this.dbName = dbName;
		this.ifExists = ifExists;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database database = newSchema.findDatabase(dbName);

		if ( database == null ) {
			if ( ifExists ) { // ignore missing databases
				return originalSchema;
			} else {
				throw new SchemaSyncError("Can't drop missing database: " + dbName);
			}
		}

		newSchema.getDatabases().remove(database);
		return newSchema;
	}

}
