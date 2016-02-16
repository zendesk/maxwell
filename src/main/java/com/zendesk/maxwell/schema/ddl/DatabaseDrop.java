package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatabaseDrop extends SchemaChange {
	public String database;

	@JsonProperty("if-exists")
	public boolean ifExists;

	public DatabaseDrop() { }
	public DatabaseDrop(String database, boolean ifExists) {
		this.database = database;
		this.ifExists = ifExists;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(database);

		if ( d == null ) {
			if ( ifExists ) { // ignore missing databases
				return originalSchema;
			} else {
				throw new SchemaSyncError("Can't drop missing database: " + database);
			}
		}

		newSchema.getDatabases().remove(d);
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return false;
	}

}
