package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String table;
	public List<ColumnDef> columns;

	@JsonProperty("primary-key")
	public List<String> pks;
	public String charset;

	public String likeDB;
	public String likeTable;
	protected boolean ifNotExists;

	public TableCreate() {
		this.ifNotExists = false;
	}

	public TableCreate (String database, String table, boolean ifNotExists) {
		this.database = database;
		this.table = table;
		this.ifNotExists = ifNotExists;
		this.columns = new ArrayList<>();
		this.pks = new ArrayList<>();
	}

	@Override
	public TableCreate resolve(Schema schema) throws SchemaSyncError {
		Database d = schema.findDatabase(this.database);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.database);


		if ( ifNotExists && d.hasTable(table) )
			return null;

		TableCreate resolved = new TableCreate(database, table, false);

		if ( likeDB != null && likeTable != null ) {
			resolveLikeTable(schema, resolved);
		} else {
			resolved.columns = columns;
			resolved.pks = pks;
			resolveCharsets(d.getCharset(), resolved);
		}

		return resolved;
	}

	private void resolveLikeTable(Schema schema, TableCreate resolved) throws SchemaSyncError {
		Database sourceDB = schema.findDatabase(likeDB);

		if ( sourceDB == null )
			throw new SchemaSyncError("Couldn't find database " + likeDB);

		Table sourceTable = sourceDB.findTable(likeTable);
		if ( sourceTable == null )
			throw new SchemaSyncError("Couldn't find table " + likeDB + "." + likeTable);

		sourceTable = sourceTable.copy();

		resolved.columns = sourceTable.getColumnList();
		resolved.pks = sourceTable.getPKList();
		resolved.charset = sourceTable.getCharset();
	}

	private void resolveCharsets(String dbCharset, TableCreate resolved) {
		if ( this.charset != null )
			resolved.charset = this.charset;
		else
			// inherit charset from database
			resolved.charset = dbCharset;

		for ( ColumnDef c : resolved.columns ) {
			if ( c instanceof StringColumnDef ) {
				StringColumnDef sc = (StringColumnDef) c;
				if ( sc.charset == null )
					sc.charset = resolved.charset;
			}
		}
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database d = newSchema.findDatabase(this.database);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.database);

		if ( d.hasTable(this.table) )
			throw new SchemaSyncError("Unexpectedly asked to create existing table " + this.table);

		Table t = d.buildTable(this.table, this.charset, this.columns, this.pks);
		return newSchema;
	}

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.table);
		}
	}

}
