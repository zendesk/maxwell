package com.zendesk.maxwell.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sql {
	public static String inListSQL(int count) {
		return "(" + StringUtils.repeat("?", ", ", count) + ")";
	}

	public static void prepareInList(PreparedStatement s, int offset, Iterable<?> list) throws SQLException {
		for ( Object o : list ) {
			s.setObject(offset++, o);
		}
	}
}
