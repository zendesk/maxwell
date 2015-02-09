package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SchemaCapturer {
	private final Connection connection;

	private final String[] alwaysExclude = {"performance_schema", "information_schema", "mysql", "test"};
	private final HashSet<String> excludeDatabases;
	private final HashSet<String> includeDatabases;

	private final PreparedStatement infoSchemaStmt;
	private final String INFORMATION_SCHEMA_SQL =
			"SELECT * FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?";

	public SchemaCapturer(Connection c) throws SQLException {
		this.excludeDatabases = new HashSet<String>();
		this.includeDatabases = new HashSet<String>();
		this.connection = c;
		this.infoSchemaStmt = connection.prepareStatement(INFORMATION_SCHEMA_SQL);

		for (String s : alwaysExclude) {
			this.excludeDatabases.add(s);
		}
	}

	public SchemaCapturer(Connection c, String dbName) throws SQLException {
		this(c);
		this.includeDatabases.add(dbName);
	}

	public Schema capture() throws SQLException {
		ArrayList<Database> databases = new ArrayList<Database>();
		for ( String dbName : selectFirst("show databases")) {
			if ( includeDatabases.size() > 0 && !includeDatabases.contains(dbName))
				continue;

			if ( excludeDatabases.contains(dbName) )
				continue;

			String showTableSQL = "show tables from " + dbName;

			ArrayList<Table> tables = new ArrayList<Table>();

			for ( String table : selectFirst(showTableSQL) ) {
				tables.add(captureTable(dbName, table));
			}
			databases.add(new Database(dbName, tables));
		}
		return new Schema(databases);
	}


	private Table captureTable(String dbName, String tableName) throws SQLException {
		infoSchemaStmt.setString(1, dbName);
		infoSchemaStmt.setString(2, tableName);
		return new Table(dbName, tableName, infoSchemaStmt.executeQuery());
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
