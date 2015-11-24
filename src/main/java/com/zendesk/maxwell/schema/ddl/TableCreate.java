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
	public ArrayList<String> pks;
	public String encoding;

	public String likeDB;
	public String likeTable;
	public final boolean ifNotExists;

	public TableCreate (String dbName, String tableName, boolean ifNotExists) {
		this.dbName = dbName;
		this.tableName = tableName;
		this.ifNotExists = ifNotExists;
		this.columns = new ArrayList<>();
		this.pks = new ArrayList<>();
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.dbName);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.dbName);

		if ( likeDB != null && likeTable != null ) {
			applyCreateLike(newSchema, d);
		} else {
			Table existingTable = d.findTable(this.tableName);
			if (existingTable != null) {
				if (ifNotExists) {
					return originalSchema;
				} else {
					throw new SchemaSyncError("Unexpectedly asked to create existing table " + this.tableName);
				}
			}
			Table t = d.buildTable(this.tableName, this.encoding, this.columns, this.pks);
			t.setDefaultColumnEncodings();
		}

		return newSchema;
	}

	private void applyCreateLike(Schema newSchema, Database d) throws SchemaSyncError {
		Database sourceDB = newSchema.findDatabase(likeDB);

		if ( sourceDB == null )
			throw new SchemaSyncError("Couldn't find database " + likeDB);

		Table sourceTable = sourceDB.findTable(likeTable);
		if ( sourceTable == null )
			throw new SchemaSyncError("Couldn't find table " + likeDB + "." + likeTable);

		Table t = sourceTable.copy();
		t.rename(this.tableName);
		d.addTable(t);
	}
}
