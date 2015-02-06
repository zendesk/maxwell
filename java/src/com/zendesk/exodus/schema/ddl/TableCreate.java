package com.zendesk.exodus.schema.ddl;

import java.util.ArrayList;

import com.zendesk.exodus.schema.Database;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String tableName;
	public ArrayList<ColumnDef> columns;
	public String encoding;

	public TableCreate () {
		this.columns = new ArrayList<>();
	}

	@Override
	Schema apply(String currentDatabase, Schema originalSchema) throws SchemaSyncError {
		String db = database == null ? currentDatabase : database;

		Database d = originalSchema.findDatabase(db);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + d);

		Table t = new Table(db, this.tableName, this.columns);
		d.getTableList().add(t);

		return null;
	}
}
