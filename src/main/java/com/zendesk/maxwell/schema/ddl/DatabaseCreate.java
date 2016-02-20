package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseCreate extends SchemaChange {
	public String database;
	private boolean ifNotExists;
	public String charset;

	public DatabaseCreate() { } // for deserialization
	public DatabaseCreate(String database, boolean ifNotExists, String charset) {
		this.database = database;
		this.ifNotExists = ifNotExists;
		this.charset = charset;
	}

	@Override
	public DatabaseCreate resolve(Schema schema) throws SchemaSyncError {
		if ( schema.hasDatabase(database) ) {
			if ( ifNotExists )
				return null;
			else
				throw new SchemaSyncError("Unexpectedly asked to create existing database " + database);
		}

		String chset;
		if ( this.charset == null )
			chset = schema.getCharset();
		else
			chset = this.charset;

		return new DatabaseCreate(database, false, chset);
	}


	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		if ( newSchema.hasDatabase(database) && !ifNotExists )
			throw new SchemaSyncError("Unexpectedly asked to create existing database " + database);

		newSchema.addDatabase(new Database(database, charset));
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return false;
	}

}
