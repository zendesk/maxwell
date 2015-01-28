package com.zendesk.exodus.schema;

import java.util.ArrayList;
import java.util.List;

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
}
