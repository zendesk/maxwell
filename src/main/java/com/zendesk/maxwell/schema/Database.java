package com.zendesk.maxwell.schema;

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

	public Database(String name, List<Table> tables) {
		this.name = name;
		if ( tables == null )
			this.tableList = new ArrayList<>();
		else
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

	private void diffTableList(List<String> diffs, Database a, Database b, String nameA, String nameB, boolean recurse) {
		for ( Table t : a.getTableList() ) {
			Table other = b.findTable(t.getName());
			if ( other == null )
				diffs.add("database " + a.getName() + " did not contain table " + t.getName() + " in " + nameB);
			else if ( recurse )
				t.diff(diffs, other, nameA, nameB);
		}
	}

	public void diff(List<String> diffs, Database other, String nameA, String nameB) {
		diffTableList(diffs, this, other, nameA, nameB, true);
		diffTableList(diffs, other, this, nameB, nameA, false);
	}
}
