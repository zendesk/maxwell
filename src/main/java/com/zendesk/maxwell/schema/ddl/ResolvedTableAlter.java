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

	@JsonProperty("def")
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
	public void apply(Schema schema) throws InvalidSchemaError {
		Database oldDatabase = schema.findDatabaseOrThrow(this.database);
		Table table = oldDatabase.findTableOrThrow(this.table);

		Database newDatabase;
		if ( this.database.equals(newTable.database) )
			newDatabase = oldDatabase;
		else
			newDatabase = schema.findDatabaseOrThrow(newTable.database);

		oldDatabase.removeTable(this.table);
		newDatabase.addTable(newTable);
	}
}

