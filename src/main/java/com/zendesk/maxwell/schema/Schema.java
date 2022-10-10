package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;


public class Schema {
	private final LinkedHashMap<String, Database> dbMap;
	private final String charset;
	private final CaseSensitivity sensitivity;

	public Schema(List<Database> databases, String charset, CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
		this.charset = charset;
		this.dbMap = new LinkedHashMap<>();

		for ( Database d : databases )
			addDatabase(d);
	}

	public Collection<Database> getDatabases() { return Collections.unmodifiableCollection(this.dbMap.values()); }

	public List<String> getDatabaseNames () {
		ArrayList<String> names = new ArrayList<>(this.dbMap.size());

		for ( Database d : this.dbMap.values() ) {
			names.add(d.getName());
		}
		return names;
	}

	public Database findDatabase(String string) {
		return this.dbMap.get(getNormalizedDbName(string));
	}

	private String getNormalizedDbName(String dbName) {
		if (dbName == null) {
			return null;
		}
		if (sensitivity == CaseSensitivity.CASE_SENSITIVE) {
			return dbName;
		} else {
			return dbName.toLowerCase();
		}
	}

	public Database findDatabaseOrThrow(String name) throws InvalidSchemaError {
		Database d = findDatabase(name);
		if ( d == null )
			throw new InvalidSchemaError("Couldn't find database '" + name + "'");
		return d;
	}

	public boolean hasDatabase(String string) {
		return findDatabase(string) != null;
	}

	public void addDatabase(Database d) {
		d.setSensitivity(sensitivity);
		this.dbMap.put(getNormalizedDbName(d.getName()), d);
	}

	public void removeDatabase(Database d) {
		this.dbMap.remove(getNormalizedDbName(d.getName()));
	}

	public static boolean charsetEquals(String thisCharset, String thatCharset) {
		if ( thisCharset == null || thatCharset == null ) {
			return thisCharset == thatCharset;
		}

		thisCharset = thisCharset.toLowerCase();
		thatCharset = thatCharset.toLowerCase();

		if ( thisCharset.equals("utf8mb3") )
			thisCharset = "utf8";

		if ( thatCharset.equals("utf8mb3") )
			thatCharset = "utf8";

		return thisCharset.equals(thatCharset);
	}

	private void diffDBList(List<String> diff, Schema a, Schema b, String nameA, String nameB, boolean recurse) {
		for ( Database d : a.dbMap.values() ) {
			Database matchingDB = b.findDatabase(d.getName());

			if ( matchingDB == null )
				diff.add("-- Database " + d.getName() + " did not exist in " + nameB);
			else if ( recurse )
				d.diff(diff, matchingDB, nameA, nameB);
		}

	}

	public List<String> diff(Schema that, String thisName, String thatName) {
		List<String> diff = new ArrayList<>();

		diffDBList(diff, this, that, thisName, thatName, true);
		diffDBList(diff, that, this, thatName, thisName, false);
		return diff;
	}

	public boolean equals(Schema that) {
		return diff(that, "a", "b").size() == 0;
	}

	public String getCharset() {
		return charset;
	}

	public CaseSensitivity getCaseSensitivity() {
		return sensitivity;
	};

	public List<Pair<FullColumnDef, FullColumnDef>> matchColumns(Schema thatSchema) {
		ArrayList<Pair<FullColumnDef, FullColumnDef>> list = new ArrayList<>();

		for ( Database thisDatabase : this.getDatabases() ) {
			Database thatDatabase = thatSchema.findDatabase(thisDatabase.getName());

			if ( thatDatabase == null )
				continue;

			for ( Table thisTable : thisDatabase.getTableList() ) {
				Table thatTable = thatDatabase.findTable(thisTable.getName());

				if ( thatTable == null )
					continue;

				for ( ColumnDef thisColumn : thisTable.getColumnList() ) {
					ColumnDef thatColumn = thatTable.findColumn(thisColumn.getName());
					if ( thatColumn != null )
						list.add(Pair.of(
								new FullColumnDef(thisDatabase, thisTable, thisColumn),
								new FullColumnDef(thatDatabase, thatTable, thatColumn)
						));
				}
			}
		}
		return list;
	}

	public static class FullColumnDef {
		private final Database db;
		private final Table table;
		private final ColumnDef columnDef;

		public FullColumnDef(Database db, Table table, ColumnDef columnDef) {
			this.db = db;
			this.table = table;
			this.columnDef = columnDef;
		}

		public Database getDb() {
			return db;
		}

		public Table getTable() {
			return table;
		}

		public ColumnDef getColumnDef() {
			return columnDef;
		}
	}
}
