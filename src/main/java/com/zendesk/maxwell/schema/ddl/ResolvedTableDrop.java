package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

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
}
