package com.zendesk.maxwell.schema.ddl;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class TableAlter extends SchemaChange {
	public String database;
	public String table;
	public ArrayList<ColumnMod> columnMods;
	public String newTableName;
	public String newDatabase;

	public String convertCharset;
	public String defaultCharset;

	public List<String> pks;

	public TableAlter() {
		this.columnMods = new ArrayList<>();
	}

	public TableAlter(String database, String table) {
		this();
		this.database = database;
		this.table = table;
	}

	@Override
	public String toString() {
		return "TableAlter<database: " + database + ", table:" + table + ">";
	}

	@Override
	public ResolvedTableAlter resolve(Schema schema) throws SchemaSyncError {
		Database database = schema.findDatabase(this.database);
		if ( database == null ) {
			throw new SchemaSyncError("Couldn't find database: " + this.database);
		}

		Table oldTable = database.findTable(this.table);
		if ( oldTable == null ) {
			throw new SchemaSyncError("Couldn't find table: " + this.database + "." + this.table);
		}

		Table table = oldTable.copy();

		if ( newTableName != null && newDatabase != null ) {
			Database destDB = schema.findDatabase(this.newDatabase);
			if ( destDB == null )
				throw new SchemaSyncError("Couldn't find database " + this.database);

			table.rename(newTableName);
		}

		for (ColumnMod mod : columnMods) {
			mod.apply(table);
		}

		if ( convertCharset != null ) {
			for ( ColumnDef c : table.getColumnList() ) {
				if ( c instanceof StringColumnDef ) {
					StringColumnDef sc = (StringColumnDef) c;
					if ( !sc.getCharset().toLowerCase().equals("binary") )
						sc.setCharset(convertCharset);
				}
			}
		}

		if ( this.pks != null ) {
			table.setPKList(this.pks);
		}
		table.setDefaultColumnCharsets();

		return new ResolvedTableAlter(this.database, this.table, oldTable, table);
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		throw new RuntimeException("resolve TableAlter before calling apply()");
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
