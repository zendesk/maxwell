package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

import java.sql.ResultSet;
import java.sql.SQLException;

public class YearColumnDef extends ColumnDef {
	public YearColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_YEAR;
	}

	@Override
	public String toSQL(Object value) {
		return ((Integer)value).toString();
	}

	@Override
	public Object getObjectFromResultSet(ResultSet resultSet, int columnIndex) throws SQLException {
		return resultSet.getInt(columnIndex);
	}
}
