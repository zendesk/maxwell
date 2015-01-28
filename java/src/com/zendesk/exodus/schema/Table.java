package com.zendesk.exodus.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.zendesk.exodus.schema.column.AbstractColumn;

public class Table {
	private List<AbstractColumn> columnList;
	private final String name;

	public Table(String name) {
		this.name = name;
	}

	public Table(String name, ResultSet r) throws SQLException {
		this(name);
		this.columnList = buildColumnsFromResultSet(r);
	}

	private List<AbstractColumn> buildColumnsFromResultSet(ResultSet r) throws SQLException {
		List<AbstractColumn> columns = new ArrayList<AbstractColumn>();

		while(r.next()) {
			String colName    = r.getString("COLUMN_NAME");
			String colType    = r.getString("DATA_TYPE");
			String colEnc     = r.getString("CHARACTER_SET_NAME");
			int colPos        = r.getInt("ORDINAL_POSITION");
			boolean colSigned = !r.getString("COLUMN_TYPE").matches(" unsigned$");


			columns.add(AbstractColumn.build(this.name, colName, colEnc, colType, colPos, colSigned));
		}

		return columns;

	}

	public List<AbstractColumn> getColumnList() {
		return columnList;
	}

	public String getName() {
		return this.name;
	}
}
