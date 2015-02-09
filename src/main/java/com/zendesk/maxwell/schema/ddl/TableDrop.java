package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class TableDrop extends SchemaChange {
	final String dbName;
	final String tableName;

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
