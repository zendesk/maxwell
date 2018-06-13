package com.zendesk.maxwell.core.schema.ddl;

import com.zendesk.maxwell.core.schema.InvalidSchemaError;
import com.zendesk.maxwell.core.schema.Database;
import com.zendesk.maxwell.core.schema.Schema;

public class ResolvedTableDrop extends ResolvedSchemaChange {
	public String database;
	public String table;

	public ResolvedTableDrop() { }
	public ResolvedTableDrop(String database, String table) {
		this.database = database;
		this.table = table;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(this.database);
		d.findTableOrThrow(this.table);

		d.removeTable(this.table);
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return table;
	}
}
