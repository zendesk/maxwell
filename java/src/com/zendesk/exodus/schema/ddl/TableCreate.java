package com.zendesk.exodus.schema.ddl;

import java.util.ArrayList;

import com.zendesk.exodus.schema.columndef.ColumnDef;

public class TableCreate {
	public String database;
	public String tableName;
	public ArrayList<ColumnDef> columns;
	public String encoding;

	public TableCreate () {
		this.columns = new ArrayList<>();
	}
}
