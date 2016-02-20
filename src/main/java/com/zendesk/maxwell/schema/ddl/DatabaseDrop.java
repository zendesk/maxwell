package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatabaseDrop extends SchemaChange {
	public String database;
	private boolean ifExists;

	public DatabaseDrop() { }
	public DatabaseDrop(String database, boolean ifExists) {
		this.database = database;
		this.ifExists = ifExists;
	}

	@Override
	public DatabaseDrop resolve(Schema schema) throws SchemaSyncError {
		if ( !schema.hasDatabase(database) ) {
			if ( ifExists )
				return null;
			else
				throw new SchemaSyncError("Can't drop missing database: " + database);
		} else {
			return new DatabaseDrop(this.database, false);
		}
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		if ( !newSchema.hasDatabase(database) && !ifExists )
			throw new SchemaSyncError("Can't drop missing database: " + database);

		newSchema.getDatabases().remove(newSchema.findDatabase(database));
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return false;
	}

}
