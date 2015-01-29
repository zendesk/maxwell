package com.zendesk.exodus.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.zendesk.exodus.schema.columndef.ColumnDef;

public class Table {
	private List<ColumnDef> columnList;
	private final String name;

	public Table(String name) {
		this.name = name;
	}

	public Table(String name, ResultSet r) throws SQLException {
		this(name);
		this.columnList = buildColumnsFromResultSet(r);
	}

	private List<ColumnDef> buildColumnsFromResultSet(ResultSet r) throws SQLException {
		List<ColumnDef> columns = new ArrayList<ColumnDef>();

		while(r.next()) {
			String colName    = r.getString("COLUMN_NAME");
			String colType    = r.getString("DATA_TYPE");
			String colEnc     = r.getString("CHARACTER_SET_NAME");
			int colPos        = r.getInt("ORDINAL_POSITION") - 1;
			boolean colSigned = !r.getString("COLUMN_TYPE").matches(" unsigned$");


			columns.add(ColumnDef.build(this.name, colName, colEnc, colType, colPos, colSigned));
		}

		return columns;

	}

	public List<ColumnDef> getColumnList() {
		return columnList;
	}

	public String getName() {
		return this.name;
	}
}
