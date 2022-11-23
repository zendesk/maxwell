package com.zendesk.maxwell.schema;

/* represents the schema of the `maxwell` databases, and contains code around upgrading
 * and managing that schema
 *
 * TODO: move all this into MysqlSchemaStore
 */

import java.sql.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class SchemaStoreSchema {
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStoreSchema.class);

	public static void ensureMaxwellSchema(Connection connection, String schemaDatabaseName) throws SQLException, IOException, InvalidSchemaError {
		if ( !storeDatabaseExists(connection, schemaDatabaseName) ) {
			createStoreDatabase(connection, schemaDatabaseName);
		}
	}

	private static boolean storeDatabaseExists(Connection connection, String schemaDatabaseName) throws SQLException {
		try ( Statement s = connection.createStatement() ) {
			try ( ResultSet rs = s.executeQuery("show databases like '" + schemaDatabaseName + "'") ) {
				if (!rs.next())
					return false;
			}

			try (ResultSet rs = s.executeQuery("show tables from `" + schemaDatabaseName + "` like 'schemas'") ) {
				return rs.next();
			}
		}
	}

	private static void executeSQLInputStream(Connection connection, InputStream schemaSQL, String schemaDatabaseName) throws SQLException, IOException {
		BufferedReader r = new BufferedReader(new InputStreamReader(schemaSQL));
		String sql = "", line;

		if ( schemaDatabaseName != null ) {
			try ( Statement stmt = connection.createStatement() ) {
				stmt.execute("CREATE DATABASE IF NOT EXISTS `" + schemaDatabaseName + "`");
			}
			if (!schemaDatabaseName.equals(connection.getCatalog()))
				connection.setCatalog(schemaDatabaseName);
		}

		while ((line = r.readLine()) != null) {
			sql += line + "\n";
		}
		for (String statement : StringUtils.splitByWholeSeparator(sql, "\n\n")) {
			if (statement.length() == 0)
				continue;

			try ( Statement stmt = connection.createStatement() ) {
				stmt.execute(statement);
			}
		}
	}

	private static void createStoreDatabase(Connection connection, String schemaDatabaseName) throws SQLException, IOException {
		LOGGER.info("Creating " + schemaDatabaseName + " database");
		executeSQLInputStream(connection, SchemaStoreSchema.class.getResourceAsStream("/sql/maxwell_schema.sql"), schemaDatabaseName);
		executeSQLInputStream(connection, SchemaStoreSchema.class.getResourceAsStream("/sql/maxwell_schema_bootstrap.sql"), schemaDatabaseName);
		executeSQLInputStream(connection, SchemaStoreSchema.class.getResourceAsStream("/sql/maxwell_schema_heartbeats.sql"), schemaDatabaseName);
	}

	private static HashMap<String, String> getTableColumns(String table, Connection c) throws SQLException {
		HashMap<String, String> map = new HashMap<>();
		try ( Statement stmt = c.createStatement();
		      ResultSet rs = stmt.executeQuery("show columns from `" + table + "`") ) {
			while (rs.next()) {
				map.put(rs.getString("Field"), rs.getString("Type"));
			}
		}
		return map;
	}

	private static ArrayList<String> getMaxwellTables(Connection c) throws SQLException {
		ArrayList<String> l = new ArrayList<>();

		try ( Statement stmt = c.createStatement();
			  ResultSet rs = stmt.executeQuery("show tables") ) {
			while (rs.next()) {
				l.add(rs.getString(1));
			}
		}
		return l;
	}

	private static void performAlter(Connection c, String sql) throws SQLException {
		LOGGER.info("Maxwell is upgrading its own schema: '" + sql + "'");
		try ( Statement stmt = c.createStatement() ) {
			stmt.execute(sql);
		}
	}

	public static void upgradeSchemaStoreSchema(Connection c) throws SQLException, IOException {
		ArrayList<String> maxwellTables = getMaxwellTables(c);
		if ( !getTableColumns("positions", c).containsKey("vitess_gtid") ) {
			performAlter(c, "alter table `positions` add column vitess_gtid text charset latin1");
		}

		if ( !getTableColumns("schemas", c).containsKey("deleted") ) {
			performAlter(c, "alter table `schemas` add column deleted tinyint(1) not null default 0");
		}

		if ( !getTableColumns("schemas", c).containsKey("gtid_set") ) {
			performAlter(c, "alter table `schemas` add column gtid_set varchar(4096)");
		}

		if ( !maxwellTables.contains("bootstrap") )  {
			LOGGER.info("adding bootstrap tables to the maxwell schema.");
			InputStream is = MysqlSavedSchema.class.getResourceAsStream("/sql/maxwell_schema_bootstrap.sql");
			executeSQLInputStream(c, is, null);
		}

		if ( !getTableColumns("bootstrap", c).containsKey("total_rows") ) {
			performAlter(c, "alter table `bootstrap` add column total_rows bigint unsigned not null default 0 after inserted_rows");
			performAlter(c, "alter table `bootstrap` modify column inserted_rows bigint unsigned not null default 0");
		}

		if ( !getTableColumns("bootstrap", c).containsKey("where_clause") ) {
			performAlter(c, "alter table `bootstrap` add column where_clause varchar(1024)");
		}

		HashMap<String, String> schemaColumns = getTableColumns("schemas", c);
		if ( !schemaColumns.containsKey("charset")) {
			String[] charsetTables = { "schemas", "databases", "tables", "columns" };
			for ( String table : charsetTables ) {
				performAlter(c, "alter table `" + table + "` change `encoding` `charset` varchar(255)");
			}
		}

		if ( !schemaColumns.containsKey("base_schema_id"))
			performAlter(c, "alter table `schemas` add column base_schema_id int unsigned NULL default NULL after binlog_position");

		if ( !schemaColumns.containsKey("deltas"))
			performAlter(c, "alter table `schemas` add column deltas mediumtext charset 'utf8' NULL default NULL after base_schema_id");

		if ( !schemaColumns.containsKey("version")) {
			performAlter(c, "alter table `schemas` add column `version` smallint unsigned not null default 0 after `charset`");
		}

		if ( !getTableColumns("positions", c).containsKey("client_id") ) {
			performAlter(c, "alter table `positions` add column `client_id` varchar(255) charset 'latin1' not null default 'maxwell'");
			performAlter(c, "alter table `positions` drop primary key, add primary key(`server_id`, `client_id`)");
		}

		if ( !getTableColumns("positions", c).containsKey("gtid_set") ) {
			performAlter(c, "alter table `positions` add column gtid_set varchar(4096)");
		}

		if ( !getTableColumns("positions", c).containsKey("heartbeat_at") ) {
			// Note: unused as of 64a6a30074e3509ed9ed102a149bf5ca844f5df5; will be removed in the future
			performAlter(c, "alter table `positions` add column `heartbeat_at` bigint null default null");
		}

		if ( !getTableColumns("positions", c).containsKey("last_heartbeat_read") ) {
			performAlter(c, "alter table `positions` add column `last_heartbeat_read` bigint null default null");
		}

		if ( !getTableColumns("columns", c).containsKey("column_length") ) {
			performAlter(c, "alter table `columns` add column `column_length` tinyint unsigned");
		}

		if ( !schemaColumns.containsKey("position_sha") ) {
			performAlter(c, "alter table `schemas` add column `position_sha` char(40) charset 'latin1' null default null, add unique index(`position_sha`)");
			backfillPositionSHAs(c);
		}

		if ( !maxwellTables.contains("heartbeats") )  {
			LOGGER.info("adding heartbeats table to the maxwell schema.");
			InputStream is = MysqlSavedSchema.class.getResourceAsStream("/sql/maxwell_schema_heartbeats.sql");
			executeSQLInputStream(c, is, null);
		}

		if ( !schemaColumns.containsKey("last_heartbeat_read") ) {
			// default 0 makes sorting easier (rows before this migration are older than those after)
			performAlter(c, "alter table `schemas` add column `last_heartbeat_read` bigint null default 0");
		}

		if ( !getTableColumns("bootstrap", c).containsKey("client_id") ) {
			performAlter(c, "alter table `bootstrap` add column `client_id` varchar(255) charset 'latin1' not null default 'maxwell'");
		}

		if ( !getTableColumns("bootstrap", c).containsKey("comment") ) {
			performAlter(c, "alter table `bootstrap` add column `comment` varchar(255) charset 'utf8' default null");
		}

		if ( !getTableColumns("bootstrap", c).get("where_clause").equals("text") ) {
			performAlter(c, "alter table `bootstrap` modify where_clause text default null");
		}

		// bigint conversions
		HashMap<String, String> columnsColumns = getTableColumns("columns", c);
		if ( !columnsColumns.get("id").startsWith("bigint")
				|| !columnsColumns.get("schema_id").startsWith("bigint")
				|| !columnsColumns.get("table_id").startsWith("bigint")) {
			performAlter(c, "alter table `columns` " +
					"modify id bigint NOT NULL AUTO_INCREMENT, " +
					"modify schema_id bigint, " +
					"modify table_id bigint");
		}

		HashMap<String, String> tablesColumns = getTableColumns("tables", c);
		if ( !tablesColumns.get("id").startsWith("bigint")
				|| !tablesColumns.get("schema_id").startsWith("bigint")
				|| !tablesColumns.get("database_id").startsWith("bigint")) {
			performAlter(c, "alter table `tables` " +
					"modify id bigint NOT NULL AUTO_INCREMENT, " +
					"modify schema_id bigint, " +
					"modify database_id bigint");
		}

		HashMap<String, String> databasesColumns = getTableColumns("databases", c);
		if ( !databasesColumns.get("id").startsWith("bigint")
				|| !databasesColumns.get("schema_id").startsWith("bigint")) {
			performAlter(c, "alter table `databases` " +
					"modify id bigint NOT NULL AUTO_INCREMENT, " +
					"modify schema_id bigint");
		}

		HashMap<String, String> schemaColumns2 = getTableColumns("schemas", c);
		if ( !schemaColumns2.get("id").startsWith("bigint")
				|| !schemaColumns2.get("base_schema_id").startsWith("bigint")) {
			performAlter(c, "alter table `schemas` " +
					"modify id bigint NOT NULL AUTO_INCREMENT, " +
					"modify base_schema_id bigint");
		}

		if ( !getTableColumns("bootstrap", c).get("id").startsWith("bigint")) {
			performAlter(c, "alter table `bootstrap` modify id bigint NOT NULL AUTO_INCREMENT");
		}
		// end bigint conversions
	}

	private static void backfillPositionSHAs(Connection c) throws SQLException {
		try ( Statement stmt = c.createStatement();
		      ResultSet rs = stmt.executeQuery("select * from `schemas`") ) {
			while (rs.next()) {
				Long id = rs.getLong("id");
				Position position = new Position(
						new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file")),
						rs.getLong("last_heartbeat_read")
				);
				String sha = MysqlSavedSchema.getSchemaPositionSHA(rs.getLong("server_id"), position);
				try ( Statement stmtUpdate = c.createStatement() ) { // statements cannot interleave ResultSets, so we need a new statement
					stmtUpdate.executeUpdate("update `schemas` set `position_sha` = '" + sha + "' where id = " + id);
				}
			}
		}
	}
}
