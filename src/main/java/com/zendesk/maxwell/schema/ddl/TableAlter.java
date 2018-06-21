package com.zendesk.maxwell.schema.ddl;

import java.util.*;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import com.zendesk.maxwell.CaseSensitivity;

public class TableAlter extends SchemaChange {
	public String database;
	public String table;
	public ArrayList<ColumnMod> columnMods;
	public String newTableName;
	public String newDatabase;

	public String convertCharset;
	public String defaultCharset;
	public List<String> pks;

	public TableAlter(String database, String table) {
		this.database = database;
		this.table = table;
		this.columnMods = new ArrayList<>();
	}

	@Override
	public String toString() {
		return "TableAlter<database: " + database + ", table:" + table + ">";
	}

	@Override
	public ResolvedTableAlter resolve(Schema schema) throws InvalidSchemaError {
		Database database = schema.findDatabaseOrThrow(this.database);
		Table oldTable = database.findTableOrThrow(this.table);

		Table table = oldTable.copy();

		if ( newTableName != null && newDatabase != null ) {
			schema.findDatabaseOrThrow(this.newDatabase);

			if ( schema.getCaseSensitivity() == CaseSensitivity.CONVERT_TO_LOWER )
				newTableName = newTableName.toLowerCase();

			table.name = newTableName;
			table.database = newDatabase;
		}

		for (ColumnMod mod : columnMods) {
			mod.apply(table);
		}

		if ( convertCharset != null ) {
			for ( StringColumnDef sc : table.getStringColumns() ) {
				if ( !sc.getCharset().toLowerCase().equals("binary") )
					sc.setCharset(convertCharset);
			}
		}

		if ( this.pks != null ) {
			table.setPKList(this.pks);
		}

		if ( this.defaultCharset != null )
			table.charset = this.defaultCharset;

		table.setDefaultColumnCharsets();

		return new ResolvedTableAlter(this.database, this.table, oldTable, table);
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
