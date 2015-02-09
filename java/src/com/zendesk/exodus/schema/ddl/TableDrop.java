package com.zendesk.exodus.schema.ddl;

import com.zendesk.exodus.schema.Database;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.Table;

public class TableDrop extends SchemaChange {
	private final String dbName;
	private final String tableName;

	public TableDrop(String dbName, String tableName) {
		this.dbName = dbName;
		this.tableName = tableName;
	}
	@Override
	Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = findDatabase(newSchema, this.dbName);
		Table t = findTable(d, this.tableName);

		d.getTableList().remove(t);
		return newSchema;
	}

}
