package com.zendesk.exodus.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaCapturer {
	private final String[] alwaysExclude = {"information_schema", "mysql", "test"};
	private final Connection connection;
	private final HashSet<String> excludeDatabases;

	public SchemaCapturer(Connection c) {
		this.excludeDatabases = new HashSet<String>();

		this.connection = c;

		for (String s : alwaysExclude) {
			excludeDatabases.add(s);
		}
	}

	public SchemaCapturer(Connection c, String[] exclude) {
		this(c);

		for (String s: exclude) {
			excludeDatabases.add(s);
		}
	}

	public Schema capture() throws SQLException {
		for ( String db : selectFirst("show databases")) {
			String showTableSQL = "show tables from " + db;
			for ( String table : selectFirst(showTableSQL) ) {
				System.out.println("gots " + db + ":" + table);
			}
		}
		return new Schema();
	}


	private List<String> selectFirst(String sql) throws SQLException {
		ResultSet rs = connection.createStatement().executeQuery(sql);
		ArrayList<String> list = new ArrayList<String>();

		while (rs.next()) {
			list.add(rs.getString(1));
		}
		return list;
	}
}
