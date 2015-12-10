package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;

import java.sql.ResultSet;
import java.sql.SQLException;

public class FloatColumnDef extends ColumnDef {
	public FloatColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("float") )
			return type == MySQLConstants.TYPE_FLOAT;
		else
			return type == MySQLConstants.TYPE_DOUBLE;
	}

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}

	@Override
	public Object getObjectFromResultSet(ResultSet resultSet, int columnIndex) throws SQLException {
		if ( getType().equals("float") ) {
			return resultSet.getFloat(columnIndex);
		} else {
			return resultSet.getDouble(columnIndex);
		}

	}
}
