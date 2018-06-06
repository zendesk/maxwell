package com.zendesk.maxwell.core.schema.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.core.config.MaxwellFilterSupport;
import com.zendesk.maxwell.core.schema.Database;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.Table;

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

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return table;
	}

	@Override
	public boolean shouldOutput(MaxwellFilter filter) {
		return MaxwellFilterSupport.matches(filter, database, oldTable.getName()) &&
				MaxwellFilterSupport.matches(filter, database, newTable.getName());
	}
}
