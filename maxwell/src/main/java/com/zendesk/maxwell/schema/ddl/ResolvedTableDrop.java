package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.*;

public class ResolvedTableDrop extends ResolvedSchemaChange {
	public String database;
	public String table;
	//Brady Auen: Added Table Object
	public Table def;

	public ResolvedTableDrop() { }
	public ResolvedTableDrop(String database, String table, Table def) {
		this.database = database;
		this.table = table;
		this.def = def;
	}

	@Override
	public Schema apply(Schema originalSchema) throws InvalidSchemaError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabaseOrThrow(this.database);
		d.findTableOrThrow(this.table);

		d.removeTable(this.table);
		return newSchema;
	}
}
