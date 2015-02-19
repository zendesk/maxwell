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
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);
	private final PreparedStatement schemaInsert, databaseInsert, tableInsert,
			columnInsert;

	public SchemaStore(Connection connection) throws SQLException {
		this.connection = connection;
		this.schemaInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`schemas` SET binlog_file = ?, binlog_position = ?, server_id = ?",
						Statement.RETURN_GENERATED_KEYS);
		this.databaseInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`databases` SET schema_id = ?, name = ?, encoding=?",
						Statement.RETURN_GENERATED_KEYS);
		this.tableInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`tables` SET schema_id = ?, database_id = ?, name = ?, encoding=?",
						Statement.RETURN_GENERATED_KEYS);
		this.columnInsert = connection
				.prepareStatement(
						"INSERT INTO `maxwell`.`columns` SET schema_id = ?, table_id = ?, name = ?, encoding=?, coltype=?, is_signed=?",
						Statement.RETURN_GENERATED_KEYS);
	}

	public SchemaStore(Connection connection, Schema schema,
			BinlogPosition position) throws SQLException {
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

	public void save() throws SQLException, IOException {
		createStoreDatabase();

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

	private void saveSchema() throws SQLException {
		Integer schemaId = executeInsert(schemaInsert, position.getFile(),
				position.getOffset(), 1);

		for (Database d : schema.getDatabases()) {
			Integer dbId = executeInsert(databaseInsert, schemaId, d.getName(), d.getEncoding());

			for (Table t : d.getTableList()) {
				Integer tableId = executeInsert(tableInsert, schemaId, dbId, t.getName(), t.getEncoding());
				for (ColumnDef c : t.getColumnList()) {
					executeInsert(columnInsert, schemaId, tableId, c.getName(),
							c.getEncoding(), c.getType(), c.getSigned() ? 1 : 0);
				}
			}
		}
	}

	private void createStoreDatabase() throws SQLException, IOException {
		InputStream schemaSQL = getClass().getResourceAsStream(
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

			this.connection.createStatement().execute(statement);
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
		ResultSet schemaRS = findSchemaID(targetPosition);

		if (schemaRS == null)
			throw new SchemaSyncError("Could not find schema for "
					+ targetPosition.getFile() + ":"
					+ targetPosition.getOffset());

		ArrayList<Database> databases = new ArrayList<>();
		this.schema = new Schema(databases);
		this.position = new BinlogPosition(schemaRS.getInt("binlog_position"),
				schemaRS.getString("binlog_file"));

		p = connection.prepareStatement("SELECT * from `maxwell`.`databases` where schema_id = ? ORDER by id");
		p.setInt(1, schemaRS.getInt("id"));
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
			int tID = tRS.getInt("id");

			restoreTable(d, tName, tID, tEncoding);
		}
		return d;
	}

	private void restoreTable(Database d, String name, int id, String encoding) throws SQLException {
		Statement s = connection.createStatement();
		Table t = d.buildTable(name, encoding);

		ResultSet cRS = s.executeQuery("SELECT * from `maxwell`.`columns` where table_id = " + id + " ORDER by id");

		int i = 0;
		while (cRS.next()) {
			ColumnDef c = ColumnDef.build(t.getName(),
					cRS.getString("name"), cRS.getString("encoding"),
					cRS.getString("coltype"), i++,
					cRS.getInt("is_signed") == 1);
			t.getColumnList().add(c);
		}
	}

	private ResultSet findSchemaID(BinlogPosition targetPosition)
			throws SQLException {
		PreparedStatement s = connection.prepareStatement(
			"SELECT * from `maxwell`.`schemas` "
			+ "WHERE (binlog_file < ?) OR (binlog_file = ? and binlog_position <= ?) "
			+ "ORDER BY id desc limit 1");

		s.setString(1, targetPosition.getFile());
		s.setString(2, targetPosition.getFile());
		s.setInt(3, targetPosition.getOffset());

		ResultSet rs = s.executeQuery();
		if (rs.next()) {
			return rs;
		} else
			return null;
	}

	public Schema getSchema() {
		return this.schema;
	}
}
