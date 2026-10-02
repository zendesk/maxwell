package com.zendesk.maxwell.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sql {
	public static String inListSQL(int count) {
		return "(" + StringUtils.repeat("?", ", ", count) + ")";
	}

	/**
	 * Backtick-quote a SQL identifier so that reserved words (`key`, `order`, ...) and
	 * exotic characters are safe to interpolate into generated SQL.
	 *
	 * mysql escapes a backtick inside a quoted identifier by doubling it, so the
	 * table foo`bar is written `foo``bar`.  This is the inverse of the unquoting the
	 * DDL parser does.
	 *
	 * @param identifier a raw (unquoted) database, table or column name
	 * @return the identifier, backtick-quoted
	 */
	public static String quoteIdentifier(String identifier) {
		return "`" + identifier.replace("`", "``") + "`";
	}

	public static void prepareInList(PreparedStatement s, int offset, Iterable<?> list) throws SQLException {
		for ( Object o : list ) {
			s.setObject(offset++, o);
		}
	}
}
