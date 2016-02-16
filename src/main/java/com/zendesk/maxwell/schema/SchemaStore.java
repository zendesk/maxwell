package com.zendesk.maxwell.schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.schema.columndef.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class SchemaStore {
	private static int maxSchemas = 5;

	private final Connection connection;
	private final String schemaDatabaseName;
	private Schema schema;
	private BinlogPosition position;
	private Long schema_id;

	public Long getSchemaID() {
		return schema_id;
	}

	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);
	private final PreparedStatement schemaInsert, databaseInsert, tableInsert;
	private final String columnInsertSQL;

	private final Long serverID;

	public SchemaStore(Connection connection, Long serverID, String dbName) throws SQLException {
		this.serverID = serverID;
		this.connection = connection;
		this.schemaDatabaseName = dbName;
		this.schemaInsert = connection
				.prepareStatement(
						"INSERT INTO `schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?, charset = ?",
						Statement.RETURN_GENERATED_KEYS);
		this.databaseInsert = connection
				.prepareStatement(
						"INSERT INTO `databases` SET schema_id = ?, name = ?, charset=?",
						Statement.RETURN_GENERATED_KEYS);
		this.tableInsert = connection
				.prepareStatement(
						"INSERT INTO `tables` SET schema_id = ?, database_id = ?, name = ?, charset=?, pk=?",
						Statement.RETURN_GENERATED_KEYS);
		this.columnInsertSQL = "INSERT INTO `columns` (schema_id, table_id, name, charset, coltype, is_signed, enum_values) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?)";
	}

	public SchemaStore(Connection connection, Long serverID, Schema schema, BinlogPosition position, String dbName) throws SQLException {
		this(connection, serverID, dbName);
		this.schema = schema;
		this.position = position;
	}

	public SchemaStore(Connection connection, Long serverID, Long schema_id, String dbName) throws SQLException {
		this(connection, serverID, dbName);
		this.schema_id = schema_id;
	}

	public static int getMaxSchemas() {
		return maxSchemas;
	}

	public static void setMaxSchemas(int maxSchemas) {
		SchemaStore.maxSchemas = maxSchemas;
	}

	private static Long executeInsert(PreparedStatement preparedStatement,
			Object... values) throws SQLException {
		for (int i = 0; i < values.length; i++) {
			preparedStatement.setObject(i + 1, values[i]);
		}
		preparedStatement.executeUpdate();

		ResultSet rs = preparedStatement.getGeneratedKeys();

		if (rs.next()) {
			return rs.getLong(1);
		} else
			return null;
	}

	public void save() throws SQLException {
		if (this.schema == null)
			throw new RuntimeException("Uninitialized schema!");

		try {
			connection.setAutoCommit(false);
			this.schema_id = saveSchema();
			connection.commit();
		} finally {
			connection.setAutoCommit(true);
		}
		if ( this.schema_id != null ) {
			deleteOldSchemas(schema_id);
		}
	}


	public Long saveSchema() throws SQLException {
		Long schemaId = executeInsert(schemaInsert, position.getFile(),
				position.getOffset(), serverID, schema.getCharset());

		ArrayList<Object> columnData = new ArrayList<Object>();

		for (Database d : schema.getDatabases()) {
			Long dbId = executeInsert(databaseInsert, schemaId, d.getName(), d.getCharset());

			for (Table t : d.getTableList()) {
				Long tableId = executeInsert(tableInsert, schemaId, dbId, t.getName(), t.getCharset(), t.getPKString());


				for (ColumnDef c : t.getColumnList()) {
					String enumValuesSQL = null;

					if ( c instanceof EnumeratedColumnDef ) {
						EnumeratedColumnDef enumColumn = (EnumeratedColumnDef) c;
						enumValuesSQL = StringUtils.join(enumColumn.getEnumValues(), ",");
					}

					columnData.add(schemaId);
					columnData.add(tableId);
					columnData.add(c.getName());

					if ( c instanceof StringColumnDef ) {
						columnData.add(((StringColumnDef) c).getCharset());
					} else {
						columnData.add(null);
					}

					columnData.add(c.getType());

					if ( c instanceof IntColumnDef ) {
						columnData.add(((IntColumnDef) c).isSigned() ? 1 : 0);
					} else if ( c instanceof BigIntColumnDef ) {
						columnData.add(((BigIntColumnDef) c).isSigned() ? 1 : 0);
					} else {
						columnData.add(0);
					}

					columnData.add(enumValuesSQL);
				}

				if ( columnData.size() > 1000 )
					executeColumnInsert(columnData);

			}
		}
		if ( columnData.size() > 0 )
			executeColumnInsert(columnData);

		return schemaId;
	}

	private void executeColumnInsert(ArrayList<Object> columnData) throws SQLException {
		String insertColumnSQL = this.columnInsertSQL;

		for (int i=1; i < columnData.size() / 7; i++) {
			insertColumnSQL = insertColumnSQL + ", (?, ?, ?, ?, ?, ?, ?)";
		}

		PreparedStatement columnInsert = connection.prepareStatement(insertColumnSQL);
		int i = 1;

		for (Object o : columnData)
			columnInsert.setObject(i++,  o);

		columnInsert.execute();
		columnData.clear();
	}

	public static void ensureMaxwellSchema(Connection connection, String schemaDatabaseName) throws SQLException, IOException, SchemaSyncError {
		if ( !SchemaStore.storeDatabaseExists(connection, schemaDatabaseName) ) {
			SchemaStore.createStoreDatabase(connection, schemaDatabaseName);
		}
	}

	private static boolean storeDatabaseExists(Connection connection, String schemaDatabaseName) throws SQLException {
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery("show databases like '" + schemaDatabaseName + "'");

		return rs.next();
	}

	private static void executeSQLInputStream(Connection connection, InputStream schemaSQL, String schemaDatabaseName) throws SQLException, IOException {
		BufferedReader r = new BufferedReader(new InputStreamReader(schemaSQL));
		String sql = "", line;

		LOGGER.info("Creating maxwell database");
		connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS `" + schemaDatabaseName + "`");
		if (!connection.getCatalog().equals(schemaDatabaseName))
			connection.setCatalog(schemaDatabaseName);
		while ((line = r.readLine()) != null) {
			sql += line + "\n";
		}
		for (String statement : StringUtils.splitByWholeSeparator(sql, "\n\n")) {
			if (statement.length() == 0)
				continue;

			connection.createStatement().execute(statement);
		}
	}

	private static void createStoreDatabase(Connection connection, String schemaDatabaseName) throws SQLException, IOException {
		executeSQLInputStream(connection, SchemaStore.class.getResourceAsStream("/sql/maxwell_schema.sql"), schemaDatabaseName);
		executeSQLInputStream(connection, SchemaStore.class.getResourceAsStream("/sql/maxwell_schema_bootstrap.sql"), schemaDatabaseName);
	}

	public static SchemaStore restore(Connection connection, MaxwellContext context) throws SQLException, SchemaSyncError {
		SchemaStore s = new SchemaStore(connection, context.getServerID(), context.getConfig().databaseName);

		s.restoreFrom(context.getInitialPosition(), context.getCaseSensitivity());

		return s;
	}

	private void restoreFrom(BinlogPosition targetPosition, CaseSensitivity sensitivity)
			throws SQLException, SchemaSyncError {
		PreparedStatement p;
		boolean shouldResave = false;
		ResultSet schemaRS = findSchema(targetPosition, this.serverID);

		if (schemaRS == null) {
			// old versions of Maxwell had a bug where they set every server_id to 1.
			// try to upgrade.

			schemaRS = findSchema(targetPosition, 1L);

			if ( schemaRS == null )
				throw new SchemaSyncError("Could not find schema for "
						+ targetPosition.getFile() + ":"
						+ targetPosition.getOffset());

			LOGGER.info("found schema with server_id == 1, re-saving...");
			shouldResave = true;
		}

		ArrayList<Database> databases = new ArrayList<>();
		this.schema = new Schema(databases, schemaRS.getString("charset"), sensitivity);
		this.position = new BinlogPosition(schemaRS.getInt("binlog_position"),
				schemaRS.getString("binlog_file"));

		LOGGER.info("Restoring schema id " + schemaRS.getInt("id") + " (last modified at " + this.position + ")");

		this.schema_id = schemaRS.getLong("id");
		p = connection.prepareStatement("SELECT * from `databases` where schema_id = ? ORDER by id");
		p.setLong(1, this.schema_id);

		ResultSet dbRS = p.executeQuery();

		while (dbRS.next()) {
			this.schema.addDatabase(restoreDatabase(dbRS.getInt("id"), dbRS.getString("name"), dbRS.getString("charset")));
		}

		if ( this.schema.findDatabase("mysql") == null ) {
			LOGGER.info("Could not find mysql db, adding it to schema");
			SchemaCapturer sc = new SchemaCapturer(connection, sensitivity, "mysql");
			Database db = sc.capture().findDatabase("mysql");
			this.schema.addDatabase(db);
			shouldResave = true;
		}

		if ( shouldResave )
			this.schema_id = saveSchema();
	}

	private Database restoreDatabase(int id, String name, String charset) throws SQLException {
		Statement s = connection.createStatement();
		Database d = new Database(name, charset);

		ResultSet tRS = s.executeQuery("SELECT * from `tables` where database_id = " + id + " ORDER by id");

		while (tRS.next()) {
			String tName = tRS.getString("name");
			String tCharset = tRS.getString("charset");
			String tPKs = tRS.getString("pk");

			int tID = tRS.getInt("id");

			restoreTable(d, tName, tID, tCharset, tPKs);
		}
		return d;
	}

	private void restoreTable(Database d, String name, int id, String charset, String pks) throws SQLException {
		Statement s = connection.createStatement();

		Table t = d.buildTable(name, charset);

		ResultSet cRS = s.executeQuery("SELECT * from `columns` where table_id = " + id + " ORDER by id");

		int i = 0;
		while (cRS.next()) {
			String[] enumValues = null;
			if ( cRS.getString("enum_values") != null )
				enumValues = StringUtils.splitByWholeSeparatorPreserveAllTokens(cRS.getString("enum_values"), ",");

			ColumnDef c = ColumnDef.build(
					cRS.getString("name"), cRS.getString("charset"),
					cRS.getString("coltype"), i++,
					cRS.getInt("is_signed") == 1,
					enumValues);
			t.addColumn(c);
		}

		if ( pks != null ) {
			List<String> pkList = Arrays.asList(StringUtils.split(pks, ','));
			t.setPKList(pkList);
		}

	}

	private ResultSet findSchema(BinlogPosition targetPosition, Long serverID)
			throws SQLException {
		LOGGER.debug("looking to restore schema at target position " + targetPosition);
		PreparedStatement s = connection.prepareStatement(
			"SELECT * from `schemas` "
			+ "WHERE deleted = 0 "
			+ "AND ((binlog_file < ?) OR (binlog_file = ? and binlog_position <= ?)) AND server_id = ? "
			+ "ORDER BY id desc limit 1");

		s.setString(1, targetPosition.getFile());
		s.setString(2, targetPosition.getFile());
		s.setLong(3, targetPosition.getOffset());
		s.setLong(4, serverID);

		ResultSet rs = s.executeQuery();
		if (rs.next()) {
			return rs;
		} else
			return null;
	}

	public Schema getSchema() {
		return this.schema;
	}

	public void setSchema(Schema s) {
		this.schema = s;
	}

	private void ensureSchemaID() {
		if ( this.schema_id == null ) {
			throw new RuntimeException("Can't destroy uninitialized schema!");
		}
	}

	public void delete() throws SQLException {
		ensureSchemaID();
		connection.createStatement().execute("update `schemas` set deleted = 1 where id = " + schema_id);
	}

	public void destroy() throws SQLException {
		ensureSchemaID();

		String[] tables = { "databases", "tables", "columns" };
		connection.createStatement().execute("delete from `schemas` where id = " + schema_id);
		for ( String tName : tables ) {
            connection.createStatement().execute(
            		"delete from `" + tName + "` where schema_id = " + schema_id
            );
		}
	}

	public boolean schemaExists(long schema_id) throws SQLException {
		if ( this.schema_id == null )
			return false;
		ResultSet rs = connection.createStatement().executeQuery("select id from `schemas` where id = " + schema_id);
		return rs.next();
	}

	public BinlogPosition getBinlogPosition() {
		return this.position;
	}

	private void deleteOldSchemas(Long currentSchemaId) throws SQLException {
		if ( maxSchemas <= 0  )
			return;

		Long toDelete = currentSchemaId - maxSchemas; // start with the highest numbered ID to delete, work downwards until we run out
		while ( toDelete > 0 && schemaExists(toDelete) ) {
			new SchemaStore(connection, serverID, toDelete, this.schemaDatabaseName).delete();
			toDelete--;
		}
	}

	/*
		for the time being, when we detect other schemas we will simply wipe them down.
		in the future, this is our moment to pick up where the master left off.
	*/
	public static void handleMasterChange(Connection c, Long serverID, String schemaDatabaseName) throws SQLException {
		PreparedStatement s = c.prepareStatement(
				"SELECT id from `schemas` WHERE server_id != ? and deleted = 0"
		);

		s.setLong(1, serverID);
		ResultSet rs = s.executeQuery();

		while ( rs.next() ) {
			Long schemaID = rs.getLong("id");
			LOGGER.info("maxwell detected schema " + schemaID + " from different server_id.  deleting...");
			new SchemaStore(c, null, schemaID, schemaDatabaseName).delete();
		}

		c.createStatement().execute("delete from `positions` where server_id != " + serverID);
	}

	private static Map<String, String> getTableColumns(String table, Connection c) throws SQLException {
		HashMap<String, String> map = new HashMap<>();
		ResultSet rs = c.createStatement().executeQuery("show columns from `" + table + "`");
		while (rs.next()) {
			map.put(rs.getString("Field"), rs.getString("Type"));
		}
		return map;
	}

	private static List<String> getMaxwellTables(Connection c) throws SQLException {
		ArrayList<String> l = new ArrayList<>();

		ResultSet rs = c.createStatement().executeQuery("show tables");
		while (rs.next()) {
			l.add(rs.getString(1));
		}
		return l;
	}

	private static void performAlter(Connection c, String sql) throws SQLException {
		LOGGER.info("Maxwell is upgrading its own schema...");
		LOGGER.info(sql);
		c.createStatement().execute(sql);
	}

	public static void upgradeSchemaStoreSchema(Connection c, String schemaDatabaseName) throws SQLException, IOException {
		if ( !getTableColumns("schemas", c).containsKey("deleted") ) {
			performAlter(c, "alter table `schemas` add column deleted tinyint(1) not null default 0");
		}

		if ( !getMaxwellTables(c).contains("bootstrap") )  {
			LOGGER.info("adding `" + schemaDatabaseName + "`.`bootstrap` to the schema.");
			InputStream is = SchemaStore.class.getResourceAsStream("/sql/maxwell_schema_bootstrap.sql");
			executeSQLInputStream(c, is, schemaDatabaseName);
		}

		if ( !getTableColumns("bootstrap", c).containsKey("total_rows") ) {
			performAlter(c, "alter table `bootstrap` add column total_rows bigint unsigned not null default 0 after inserted_rows");
			performAlter(c, "alter table `bootstrap` modify column inserted_rows bigint unsigned not null default 0");
		}

		if ( !getTableColumns("schemas", c).containsKey("charset")) {
			String[] charsetTables = { "schemas", "databases", "tables", "columns" };
			for ( String table : charsetTables ) {
				performAlter(c, "alter table `" + table + "` change `encoding` `charset` varchar(255)");
			}
		}
	}

}
