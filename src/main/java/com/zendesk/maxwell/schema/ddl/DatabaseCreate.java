package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseCreate extends SchemaChange {
	public String database;
	@JsonProperty(value = "if-not-exists")
	public boolean ifNotExists;
	public String charset;

	public DatabaseCreate() { } // for deserialization
	public DatabaseCreate(String database, boolean ifNotExists, String charset) {
		this.database = database;
		this.ifNotExists = ifNotExists;
		this.charset = charset;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Database d = originalSchema.findDatabase(database);

		if ( d != null ) {
			if ( ifNotExists )
				return originalSchema;
			else
				throw new SchemaSyncError("Unexpectedly asked to create existing database " + database);
		}

		Schema newSchema = originalSchema.copy();

		String createCharset;
		if ( charset != null )
			createCharset = charset;
		else
			createCharset = newSchema.getCharset();

		newSchema.addDatabase(new Database(database, createCharset));
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return false;
	}

}
