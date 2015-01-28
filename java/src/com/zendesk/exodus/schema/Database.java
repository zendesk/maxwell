package com.zendesk.exodus.schema;

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
}
