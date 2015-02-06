package com.zendesk.exodus.schema.ddl;

import java.util.ArrayList;

import com.zendesk.exodus.schema.Database;
import com.zendesk.exodus.schema.Schema;
import com.zendesk.exodus.schema.Table;

class SchemaSyncError extends Exception {
	public SchemaSyncError (String message) { super(message); }
	private static final long serialVersionUID = 1L;
}

public class TableAlter extends SchemaChange {
	public String dbName;
	public String tableName;
	public ArrayList<ColumnMod> columnMods;
	public String newTableName;
	public String newDatabase;

	public String convertCharset;
	public String defaultCharset;


	public TableAlter(String database) {
		this.dbName = database;
		this.columnMods = new ArrayList<>();
	}

	@Override
	public String toString() {
		return "TableAlter<database: " + dbName + ", table:" + tableName + ">";
	}

	@Override
	Schema apply(String currentDatabase, Schema originalSchema) throws SchemaSyncError {
		String db;
		Schema newSchema = originalSchema.copy();

		// if no explicit database was specified, use the current database
		db = dbName == null ? currentDatabase : dbName;
		Database database = newSchema.findDatabase(db);

		if ( database == null )
			throw new SchemaSyncError("Couldn't find database " + db);

		Table table = database.findTable(tableName);

		if ( table == null )
			throw new SchemaSyncError("Couldn't find table " + tableName);

		if ( newTableName != null ) {
			if ( newDatabase != null ) {
				table.rename(newDatabase, newTableName);
			} else {
				// weird, but this is how it works; if you're in the 'test' database
				// and say "alter table `mysql`.`bar` rename to `baz`
				// your table ends up in the 'test' database.
				table.rename(currentDatabase, newTableName);
			}
		}

		for (ColumnMod mod : columnMods) {
			mod.apply(table);
		}

		return newSchema;
	}
}