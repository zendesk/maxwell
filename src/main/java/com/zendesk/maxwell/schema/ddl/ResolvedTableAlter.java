package com.zendesk.maxwell.schema.ddl;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class ResolvedTableAlter extends ResolvedSchemaChange {
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

		Database oldDatabase = newSchema.findDatabase(this.database);
		if ( oldDatabase == null ) {
			throw new SchemaSyncError("Couldn't find database: " + this.database);
		}

		Table table = oldDatabase.findTable(this.table);
		if ( table == null ) {
			throw new SchemaSyncError("Couldn't find table: " + this.database + "." + this.table);
		}

		Database newDatabase;
		if ( this.database.equals(newTable.database) )
			newDatabase = oldDatabase;
		else {
			newDatabase = newSchema.findDatabase(newTable.database);
			if ( newDatabase == null )
				throw new SchemaSyncError("Couldn't find database: " + this.newTable.database);
		}

		oldDatabase.removeTable(this.table);
		newDatabase.addTable(newTable);
		return newSchema;
	}
}

