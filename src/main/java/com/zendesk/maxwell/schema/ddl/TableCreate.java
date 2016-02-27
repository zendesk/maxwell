package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;
import java.util.List;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String table;
	public List<ColumnDef> columns;
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
	public ResolvedTableCreate resolve(Schema schema) throws SchemaSyncError {
		Database d = schema.findDatabase(this.database);
		if ( d == null )
			throw new SchemaSyncError("Couldn't find database " + this.database);

		if ( ifNotExists && d.hasTable(table) )
			return null;

		Table table = null;
		if ( likeDB != null && likeTable != null ) {
			table = resolveLikeTable(schema);
		} else {
			table = new Table(this.database, this.table, this.charset, this.columns, this.pks);
			resolveCharsets(d.getCharset(), table);
		}

		if ( schema.getCaseSensitivity() == CaseSensitivity.CONVERT_TO_LOWER )
			table.name = table.name.toLowerCase();

		return new ResolvedTableCreate(table);
	}

	private Table resolveLikeTable(Schema schema) throws SchemaSyncError {
		Database sourceDB = schema.findDatabase(likeDB);

		if ( sourceDB == null )
			throw new SchemaSyncError("Couldn't find database " + likeDB);

		Table sourceTable = sourceDB.findTable(likeTable);
		if ( sourceTable == null )
			throw new SchemaSyncError("Couldn't find table " + likeDB + "." + likeTable);

		Table copiedTable = sourceTable.copy();
		copiedTable.database = this.database;
		copiedTable.name = this.table;

		return copiedTable;
	}

	private void resolveCharsets(String dbCharset, Table resolved) {
		if ( this.charset != null )
			resolved.charset = this.charset;
		else
			// inherit charset from database
			resolved.charset = dbCharset;

		for ( StringColumnDef c : resolved.getStringColumns() ) {
			if ( c.charset == null )
				c.charset = resolved.charset;
		}
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
