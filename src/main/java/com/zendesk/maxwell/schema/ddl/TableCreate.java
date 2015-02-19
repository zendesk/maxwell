package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String dbName;
	public String tableName;
	public ArrayList<ColumnDef> columns;
	public String encoding;

	public String likeDB;
	public String likeTable;

	public TableCreate (String dbName, String tableName) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.columns = new ArrayList<>();
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.dbName);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.dbName);

		if ( likeDB != null && likeTable != null ) {
			Database sourceDB = newSchema.findDatabase(likeDB);

			if ( sourceDB == null )
				throw new SchemaSyncError("Couldn't find database " + likeDB);

			Table sourceTable = sourceDB.findTable(likeTable);
			if ( sourceTable == null )
				throw new SchemaSyncError("Couldn't find table " + likeDB + "." + sourceTable);

			Table t = sourceTable.copy();
			t.rename(this.tableName);
			d.addTable(t);
		} else {
		    Table t = d.buildTable(this.tableName, this.encoding, this.columns);
		    t.setDefaultColumnEncodings();
		}

		return newSchema;
	}
}
