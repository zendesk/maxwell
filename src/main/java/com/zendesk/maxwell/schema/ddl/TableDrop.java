package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class TableDrop extends SchemaChange {
	final String dbName;
	final String tableName;
	final boolean ifExists;

	public TableDrop(String dbName, String tableName, boolean ifExists) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.ifExists = ifExists;
	}
	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = findDatabase(newSchema, this.dbName, ifExists);

		// it's perfectly legal to say drop table if exists `random_garbage_db`.`random_garbage_table`
		if ( d == null && ifExists)
			return newSchema;

		Table t = findTable(d, this.tableName, ifExists);

		if ( t != null )
			d.getTableList().remove(t);
		return newSchema;
	}

}
