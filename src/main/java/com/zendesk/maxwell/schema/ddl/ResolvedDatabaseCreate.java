package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;

public class ResolvedDatabaseCreate extends ResolvedSchemaChange {
	public String database;
	public String charset;

	public ResolvedDatabaseCreate() { }
	public ResolvedDatabaseCreate(String database, String charset) {
		this.database = database;
		this.charset = charset;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		if ( schema.hasDatabase(database) )
			throw new InvalidSchemaError("Unexpectedly asked to create existing database " + database);

		schema.addDatabase(new Database(database, charset));
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return null;
	}
}
