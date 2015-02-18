package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class DatabaseCreate extends SchemaChange {
	public final String dbName;
	private final boolean ifNotExists;
	public final String encoding;

	public DatabaseCreate(String dbName, boolean ifNotExists, String encoding) {
		this.dbName = dbName;
		this.ifNotExists = ifNotExists;
		this.encoding = encoding;
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
		database = new Database(dbName, encoding);

		newSchema.getDatabases().add(database);
		return newSchema;
	}

}
