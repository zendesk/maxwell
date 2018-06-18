package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;
import java.util.List;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class TableCreate extends SchemaChange {
	public String database;
	public String table;
	public List<ColumnDef> columns;
	public List<String> pks;
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
	public ResolvedTableCreate resolve(Schema schema) throws InvalidSchemaError {
		Database d = schema.findDatabaseOrThrow(this.database);

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

	private Table resolveLikeTable(Schema schema) throws InvalidSchemaError {
		Database sourceDB = schema.findDatabaseOrThrow(likeDB);
		Table sourceTable = sourceDB.findTableOrThrow(likeTable);

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

		resolved.setDefaultColumnCharsets();
	}

	@Override
	public boolean isBlacklisted(Filter filter) {
		if ( filter == null ) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.database, this.table);
		}
	}

}
