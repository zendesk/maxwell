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


	public TableAlter(String database, String tableName) {
		this.dbName = database;
		this.tableName = tableName;
		this.columnMods = new ArrayList<>();
	}

	@Override
	public String toString() {
		return "TableAlter<database: " + dbName + ", table:" + tableName + ">";
	}

	@Override
	Schema apply(Schema originalSchema) throws SchemaSyncError {
		Schema newSchema = originalSchema.copy();

		// if no explicit database was specified, use the current database
		Database database = newSchema.findDatabase(this.dbName);

		if ( database == null )
			throw new SchemaSyncError("Couldn't find database " + this.dbName);

		Table table = database.findTable(tableName);

		if ( table == null )
			throw new SchemaSyncError("Couldn't find table " + tableName);

		if ( newTableName != null && newDatabase != null ) {
			if ( ! newDatabase.equals(table.getDatabase()) ) {
				Database destDB = newSchema.findDatabase(this.newDatabase);
				if ( destDB == null )
					throw new SchemaSyncError("Couldn't find database " + this.dbName);

				database.getTableList().remove(table);
				destDB.getTableList().add(table);
			}
			table.rename(newDatabase, newTableName);
		}

		for (ColumnMod mod : columnMods) {
			mod.apply(table);
		}

		return newSchema;
	}
}