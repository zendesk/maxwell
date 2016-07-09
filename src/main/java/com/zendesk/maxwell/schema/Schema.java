package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;


public class Schema {
	private final ArrayList<Database> databases;
	private final String charset;
	private final CaseSensitivity sensitivity;

	public Schema(List<Database> databases, String charset, CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
		this.charset = charset;
		this.databases = new ArrayList<>();

		for ( Database d : databases )
			addDatabase(d);
	}

	public List<Database> getDatabases() { return this.databases; }

	public List<String> getDatabaseNames () {
		ArrayList<String> names = new ArrayList<String>();

		for ( Database d : this.databases ) {
			names.add(d.getName());
		}
		return names;
	}

	public Database findDatabase(String string) {
		for ( Database d: this.databases ) {
			if ( sensitivity == CaseSensitivity.CASE_SENSITIVE ) {
				if ( d.getName().equals(string) ) return d;
			} else {
				if ( d.getName().toLowerCase().equals(string.toLowerCase()) ) return d;
			}
		}

		return null;
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
		this.databases.add(d);
	}

	private void diffDBList(List<String> diff, Schema a, Schema b, String nameA, String nameB, boolean recurse) {
		for ( Database d : a.databases ) {
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

	public List<Pair<ColumnDef, ColumnDef>> matchColumns(Schema thatSchema) {
		ArrayList<Pair<ColumnDef, ColumnDef>> list = new ArrayList<>();

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
						list.add(Pair.of(thisColumn, thatColumn));
				}
			}
		}
		return list;
	}
}
