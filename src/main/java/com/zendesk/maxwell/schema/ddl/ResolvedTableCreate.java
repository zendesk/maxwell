package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class ResolvedTableCreate extends ResolvedSchemaChange {
	public String database;
	public String table;
	public Table def;


	public ResolvedTableCreate() {}
	public ResolvedTableCreate(Table t) {
		this.database = t.database;
		this.table = t.name;
		this.def = t;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(this.database);

		if ( d.hasTable(this.table) )
			throw new InvalidSchemaError("Unexpectedly asked to create existing table " + this.table);

		d.addTable(this.def);
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
