package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class ResolvedDatabaseCreate extends ResolvedSchemaChange {
	public String database;
	public String charset;

	public ResolvedDatabaseCreate() { }
	public ResolvedDatabaseCreate(String database, String charset) {
		this.database = database;
		this.charset = charset;
	}

	@Override
	public Schema apply(Schema originalSchema) throws InvalidSchemaError {
		Schema newSchema = originalSchema.copy();

		if ( newSchema.hasDatabase(database) )
			throw new InvalidSchemaError("Unexpectedly asked to create existing database " + database);

		newSchema.addDatabase(new Database(database, charset));
		return newSchema;
	}
}
