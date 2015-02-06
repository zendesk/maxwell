package com.zendesk.exodus.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cedarsoftware.util.io.JsonWriter;

public class Schema {
	private final ArrayList<Database> databases;

	public Schema(ArrayList<Database> databases) {
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

	public Schema copy() {
		ArrayList<Database> newDBs = new ArrayList<>();
		for ( Database d : this.databases ) {
			newDBs.add(d.copy());
		}

		return new Schema(newDBs);
	}

	public String toJSON() throws IOException {
		return JsonWriter.objectToJson(this);
	}

	public boolean equals(Schema that) throws IOException {
		return this.toJSON().equals(that.toJSON());
	}

	private void diffDBList(List<String> diff, Schema a, Schema b, String nameA, String nameB) {
		for ( Database d : a.databases ) {
			Database matchingDB = b.findDatabase(d.getName());

			if ( matchingDB == null )
				diff.add("-- Database " + d.getName() + "did not exist in " + nameB);
			else
				d.diff(diff, matchingDB, nameA, nameB);
		}

	}

	public List<String> diff(Schema that, String thatName) {
		List<String> diff = new ArrayList<>();

		diffDBList(diff, this, that, "original", thatName);
		diffDBList(diff, that, this, thatName, "original");
		return diff;
	}
}
