package com.zendesk.exodus.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.zendesk.exodus.schema.columndef.ColumnDef;

public class Table {
	private List<ColumnDef> columnList;
	private int pkIndex;
	private final String name;
	private final String database;

	public Table(String dbName, String name) {
		this.database = dbName;
		this.name = name;
	}

	public Table(String dbName, String name, ResultSet r) throws SQLException {
		this(dbName, name);
		this.columnList = buildColumnsFromResultSet(r);
	}

	private List<ColumnDef> buildColumnsFromResultSet(ResultSet r) throws SQLException {
		int i = 0;
		List<ColumnDef> columns = new ArrayList<ColumnDef>();

		while(r.next()) {
			String colName    = r.getString("COLUMN_NAME");
			String colType    = r.getString("DATA_TYPE");
			String colEnc     = r.getString("CHARACTER_SET_NAME");
			int colPos        = r.getInt("ORDINAL_POSITION") - 1;
			boolean colSigned = !r.getString("COLUMN_TYPE").matches(" unsigned$");

			// todo: compound PKs, mebbe
			if ( r.getString("COLUMN_KEY").equals("PRI") )
				this.pkIndex = i;

			columns.add(ColumnDef.build(this.name, colName, colEnc, colType, colPos, colSigned));
			i++;
		}

		return columns;
	}

	public List<ColumnDef> getColumnList() {
		return columnList;
	}

	public String getName() {
		return this.name;
	}

	public int findColumnIndex(String name) {
		int i = 0;
		for(ColumnDef c : columnList) {
			if ( c.getName().equals(name) )
				return i;
			i++;

		}
		return -1;
	}

	public int getPKIndex() {
		return this.pkIndex;
	}

	public String getDatabase() {
		return database;
	}
}
