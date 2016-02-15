package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseCreate extends SchemaChange {
	public final String dbName;
	private final boolean ifNotExists;
	public final String charset;

	public DatabaseCreate(String dbName, boolean ifNotExists, String charset) {
		this.dbName = dbName;
		this.ifNotExists = ifNotExists;
		this.charset = charset;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Database database = originalSchema.findDatabase(dbName);

		if ( database != null ) {
			if ( ifNotExists )
				return originalSchema;
			else
				throw new SchemaSyncError("Unexpectedly asked to create existing database " + dbName);
		}

		Schema newSchema = originalSchema.copy();

		String createCharset;
		if ( charset != null )
			createCharset = charset;
		else
			createCharset = newSchema.getCharset();

		database = new Database(dbName, createCharset);
		newSchema.addDatabase(database);
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return false;
	}

}
