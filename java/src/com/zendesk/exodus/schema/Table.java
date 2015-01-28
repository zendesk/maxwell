package com.zendesk.exodus.schema;

import java.util.List;

public class Table {
	private final List<Column> columnList;
	private final String name;

	Table(String name, List<Column> columns) {
		this.name = name;
		this.columnList = columns;
	}


}
