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


public class ResolvedTableAlter extends SchemaChange {
	public String database;
	public String table;

	@JsonProperty("old")
	public Table oldTable;

	@JsonProperty("new")
	public Table newTable;

	public ResolvedTableAlter() { }
	public ResolvedTableAlter(String database, String table, Table oldTable, Table newTable) {
		this();
		this.database = database;
		this.table = table;
		this.oldTable = oldTable;
		this.newTable = newTable;
	}

	@Override
	public Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		Database database = newSchema.findDatabase(this.database);
		if ( database == null ) {
			throw new SchemaSyncError("Couldn't find database: " + this.database);
		}

		Table table = database.findTable(this.table);
		if ( table == null ) {
			throw new SchemaSyncError("Couldn't find table: " + this.database + "." + this.table);
		}

		database.removeTable(this.table);
		database.addTable(newTable);
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

