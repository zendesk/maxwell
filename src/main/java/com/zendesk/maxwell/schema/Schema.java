package com.zendesk.maxwell.schema;

import java.util.ArrayList;
import java.util.List;

public class Schema {
	private final ArrayList<Database> databases;
	private final String encoding;

	public Schema(ArrayList<Database> databases, String encoding) {
		this.encoding = encoding;
		this.databases = databases;
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
			if ( d.getName().equals(string) ) {
				return d;
			}
		}

		return null;
	}

	public void addDatabase(Database d) {
		this.databases.add(d);
	}

	public Schema copy() {
		ArrayList<Database> newDBs = new ArrayList<>();
		for ( Database d : this.databases ) {
			newDBs.add(d.copy());
		}

		return new Schema(newDBs, this.encoding);
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

	public String getEncoding() {
		return encoding;
	}

}
