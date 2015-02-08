package com.zendesk.exodus.schema.ddl;

import java.util.ArrayList;

import com.zendesk.exodus.schema.Database;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.Table;
import com.zendesk.exodus.schema.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String dbName;
	public String tableName;
	public ArrayList<ColumnDef> columns;
	public String encoding;

	public TableCreate (String dbName, String tableName) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.columns = new ArrayList<>();
	}

	@Override
	Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.dbName);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.dbName);

		Table t = new Table(dbName, this.tableName, this.columns);
		d.getTableList().add(t);

		return newSchema;
	}
}
