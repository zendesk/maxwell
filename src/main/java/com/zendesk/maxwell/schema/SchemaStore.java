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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class SchemaStore {
	private final Connection connection;
	private Schema schema;
	private BinlogPosition position;
	private Long schema_id;

	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);
	private final PreparedStatement schemaInsert, databaseInsert, tableInsert;
	private final String columnInsertSQL;

	public SchemaStore(Connection connection) throws SQLException {
		this.connection = connection;
		this.schemaInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?, encoding = ?",
						Statement.RETURN_GENERATED_KEYS);
		this.databaseInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`databases` SET schema_id = ?, name = ?, encoding=?",
						Statement.RETURN_GENERATED_KEYS);
		this.tableInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`tables` SET schema_id = ?, database_id = ?, name = ?, encoding=?, pk=?",
						Statement.RETURN_GENERATED_KEYS);
		this.columnInsertSQL = "INSERT INTO `maxwell`.`columns` (schema_id, table_id, name, encoding, coltype, is_signed, enum_values) "
				+ " VALUES (?, ?, ?, ?, ?, ?, ?)";
	}

	public SchemaStore(Connection connection, Schema schema, BinlogPosition position) throws SQLException {
		this(connection);
		this.schema = schema;
		this.position = position;
	}

	private static Integer executeInsert(PreparedStatement preparedStatement,
			Object... values) throws SQLException {
		for (int i = 0; i < values.length; i++) {
			preparedStatement.setObject(i + 1, values[i]);
		}
		preparedStatement.executeUpdate();

		ResultSet rs = preparedStatement.getGeneratedKeys();

		if (rs.next()) {
			return rs.getInt(1);
		} else
			return null;
	}

	public void save() throws SQLException {
		if (this.schema == null)
			throw new RuntimeException("Uninitialized schema!");

		try {
			connection.setAutoCommit(false);
			saveSchema();
			connection.commit();
		} finally {
			connection.setAutoCommit(true);
		}
	}


	public void saveSchema() throws SQLException {
		Integer schemaId = executeInsert(schemaInsert, position.getFile(),
				position.getOffset(), 1, schema.getEncoding());

		ArrayList<Object> columnData = new ArrayList<Object>();

		for (Database d : schema.getDatabases()) {
			Integer dbId = executeInsert(databaseInsert, schemaId, d.getName(), d.getEncoding());

			for (Table t : d.getTableList()) {
				Integer tableId = executeInsert(tableInsert, schemaId, dbId, t.getName(), t.getEncoding(), t.getPKString());


				for (ColumnDef c : t.getColumnList()) {
					String [] enumValues = c.getEnumValues();
					String enumValuesSQL = null;

					if ( enumValues != null ) {
						enumValuesSQL = StringUtils.join(enumValues, ",");
					}

					columnData.add(schemaId);
					columnData.add(tableId);
					columnData.add(c.getName());
					columnData.add(c.getEncoding());
					columnData.add(c.getType());
					columnData.add(c.getSigned() ? 1 : 0);
					columnData.add(enumValuesSQL);
				}

				if ( columnData.size() > 1000 )
					executeColumnInsert(columnData);

			}
		}
		if ( columnData.size() > 0 )
			executeColumnInsert(columnData);
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

	public static void ensureMaxwellSchema(Connection connection) throws SQLException, IOException, SchemaSyncError {
		if ( SchemaStore.storeDatabaseExists(connection) ) {
			new SchemaStoreMigrations(connection).upgrade();
			return;
		}

		SchemaStore.createStoreDatabase(connection);

	}
	private static boolean storeDatabaseExists(Connection connection) throws SQLException {
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery("show databases like 'maxwell'");

		return rs.next();
	}

	private static void createStoreDatabase(Connection connection) throws SQLException, IOException {
		InputStream schemaSQL = SchemaStore.class.getResourceAsStream(
				"/sql/maxwell_schema.sql");
		BufferedReader r = new BufferedReader(new InputStreamReader(schemaSQL));
		String sql = "", line;

		LOGGER.info("Creating maxwell database");
		while ((line = r.readLine()) != null) {
			sql += line + "\n";
		}

		for (String statement : StringUtils.splitByWholeSeparator(sql, "\n\n")) {
			if (statement.length() == 0)
				continue;

			connection.createStatement().execute(statement);
		}
	}

	public static SchemaStore restore(Connection connection,
			BinlogPosition targetPosition) throws SQLException, SchemaSyncError {
		SchemaStore s = new SchemaStore(connection);

		s.restoreFrom(targetPosition);

		return s;
	}

	private void restoreFrom(BinlogPosition targetPosition)
			throws SQLException, SchemaSyncError {
		PreparedStatement p;
		ResultSet schemaRS = findSchema(targetPosition);

		if (schemaRS == null)
			throw new SchemaSyncError("Could not find schema for "
					+ targetPosition.getFile() + ":"
					+ targetPosition.getOffset());

		ArrayList<Database> databases = new ArrayList<>();
		this.schema = new Schema(databases, schemaRS.getString("encoding"));
		this.position = new BinlogPosition(schemaRS.getInt("binlog_position"),
				schemaRS.getString("binlog_file"));

		LOGGER.info("Restoring schema id " + schemaRS.getInt("id") + " (last modified at " + this.position + ")");

		this.schema_id = schemaRS.getLong("id");

		p = connection.prepareStatement("SELECT * from `maxwell`.`databases` where schema_id = ? ORDER by id");
		p.setLong(1, this.schema_id);

		ResultSet dbRS = p.executeQuery();

		while (dbRS.next()) {
			this.schema.getDatabases().add(restoreDatabase(dbRS.getInt("id"), dbRS.getString("name"), dbRS.getString("encoding")));
		}
	}

	private Database restoreDatabase(int id, String name, String encoding) throws SQLException {
		Statement s = connection.createStatement();
		Database d = new Database(name, encoding);

		ResultSet tRS = s.executeQuery("SELECT * from `maxwell`.`tables` where database_id = " + id + " ORDER by id");

		while (tRS.next()) {
			String tName = tRS.getString("name");
			String tEncoding = tRS.getString("encoding");
			String tPKs = null;
			try {
				tPKs = tRS.getString("pk");
			} catch ( SQLException e ) {}

			int tID = tRS.getInt("id");

			restoreTable(d, tName, tID, tEncoding, tPKs);
		}
		return d;
	}

	private void restoreTable(Database d, String name, int id, String encoding, String pks) throws SQLException {
		Statement s = connection.createStatement();

		Table t = d.buildTable(name, encoding);

		ResultSet cRS = s.executeQuery("SELECT * from `maxwell`.`columns` where table_id = " + id + " ORDER by id");

		int i = 0;
		while (cRS.next()) {
			String[] enumValues = null;
			if ( cRS.getString("enum_values") != null )
				enumValues = StringUtils.split(cRS.getString("enum_values"), ",");

			ColumnDef c = ColumnDef.build(t.getName(),
					cRS.getString("name"), cRS.getString("encoding"),
					cRS.getString("coltype"), i++,
					cRS.getInt("is_signed") == 1,
					enumValues);
			t.getColumnList().add(c);
		}

		if ( pks != null ) {
			List<String> pkList = Arrays.asList(StringUtils.split(pks, ','));
			t.setPK(pkList);
		}

	}

	private ResultSet findSchema(BinlogPosition targetPosition)
			throws SQLException {
		LOGGER.debug("looking to restore schema at target position " + targetPosition);
		PreparedStatement s = connection.prepareStatement(
			"SELECT * from `maxwell`.`schemas` "
			+ "WHERE (binlog_file < ?) OR (binlog_file = ? and binlog_position <= ?) "
			+ "ORDER BY id desc limit 1");

		s.setString(1, targetPosition.getFile());
		s.setString(2, targetPosition.getFile());
		s.setLong(3, targetPosition.getOffset());

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

	public void destroy() throws SQLException {
		if ( this.schema_id == null ) {
			throw new RuntimeException("Can't destroy uninitialized schema!");
		}

		String[] tables = { "databases", "tables", "columns" };

		connection.createStatement().execute("delete from maxwell.schemas where id = " + schema_id);
		for ( String tName : tables ) {
            connection.createStatement().execute(
            		"delete from maxwell." + tName + " where schema_id = " + schema_id
            );
		}
	}

	public BinlogPosition getBinlogPosition() {
		return this.position;
	}


}
