package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public class TableDrop extends SchemaChange {
	public String database;
	final String table;
	final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}
	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.database);

		// it's perfectly legal to say drop table if exists `random_garbage_db`.`random_garbage_table`
		Table t = null;
		if (d != null) {
			t = d.findTable(this.table);
		}

		if ( t == null ) {
			if ( ifExists ) { // DROP TABLE IF NOT EXISTS ; ignore missing tables
				return originalSchema;
			} else {
				throw new SchemaSyncError("Can't drop non-existant table: " + this.database + "." + this.table);
			}
		}

		d.getTableList().remove(t);
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.database, this.table);
		}
	}

}
