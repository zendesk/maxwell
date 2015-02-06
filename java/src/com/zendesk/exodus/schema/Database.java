package com.zendesk.exodus.schema;

import java.util.ArrayList;
import java.util.List;

public class Database {
	private final String name;

	public String getName() {
		return name;
	}

	public List<Table> getTableList() {
		return tableList;
	}

	private final List<Table> tableList;

	Database(String name, List<Table> tables) {
		this.name = name;
		this.tableList = tables;
	}

	public List<String> getTableNames() {
		ArrayList<String> names = new ArrayList<String>();
		for ( Table t : this.tableList ) {
			names.add(t.getName());
		}
		return names;
	}

	public Table findTable(String name) {
		for ( Table t: this.tableList ) {
			if ( t.getName().equals(name))
				return t;
		}
		return null;
	}

	public Database copy() {
		ArrayList<Table> list = new ArrayList<>();
		for ( Table t: this.tableList ) {
			list.add(t.copy());
		}
		return new Database(this.name, list);
	}
}
