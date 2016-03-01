package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String table;
	public ArrayList<ColumnDef> columns;
	public ArrayList<String> pks;
	public String charset;

	public String likeDB;
	public String likeTable;
	public final boolean ifNotExists;

	public TableCreate (String database, String table, boolean ifNotExists) {
		this.database = database;
		this.table = table;
		this.ifNotExists = ifNotExists;
		this.columns = new ArrayList<>();
		this.pks = new ArrayList<>();
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.database);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.database);

		if ( likeDB != null && likeTable != null ) {
			applyCreateLike(newSchema, d);
		} else {
			Table existingTable = d.findTable(this.table);
			if (existingTable != null) {
				if (ifNotExists) {
					return originalSchema;
				} else {
					throw new SchemaSyncError("Unexpectedly asked to create existing table " + this.table);
				}
			}
			Table t = d.buildTable(this.table, this.charset, this.columns, this.pks);
			t.setDefaultColumnCharsets();
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
		t.rename(this.table);
		d.addTable(t);
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
