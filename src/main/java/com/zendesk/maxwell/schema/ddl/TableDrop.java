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

		Database d = newSchema.findDatabase(this.dbName);

		// it's perfectly legal to say drop table if exists `random_garbage_db`.`random_garbage_table`
		Table t = null;
		if (d != null) {
			t = d.findTable(this.tableName);
		}

		if ( t == null ) {
			if ( ifExists ) { // DROP TABLE IF NOT EXISTS ; ignore missing tables
				return originalSchema;
			} else {
				throw new SchemaSyncError("Can't drop non-existant table: " + this.dbName + "." + this.tableName);
			}
		}

		d.getTableList().remove(t);
		return newSchema;
	}

}
