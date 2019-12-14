package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellMysqlStatus;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SynchronousBootstrapper {
	static final Logger LOGGER = LoggerFactory.getLogger(SynchronousBootstrapper.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;
	private final MaxwellContext context;

	private long lastInsertedRowsUpdateTimeMillis = 0;

	public SynchronousBootstrapper(MaxwellContext context) {
		this.context = context;
	}


	public void startBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		performBootstrap(task, producer, currentSchemaID);
		completeBootstrap(task, producer, currentSchemaID);
	}

	private Schema captureSchemaForBootstrap(BootstrapTask task) throws SQLException {
		try ( Connection cx = getConnection(task.database) ) {
			CaseSensitivity s = MaxwellMysqlStatus.captureCaseSensitivity(cx);
			SchemaCapturer c = new SchemaCapturer(cx, s, task.database, task.table);
			return c.capture();
		}
	}

	public void performBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		LOGGER.debug("bootstrapping requested for " + task.logString());

		Schema schema = captureSchemaForBootstrap(task);
		Database database = findDatabase(schema, task.database);
		Table table = findTable(task.table, database);

		producer.push(bootstrapStartRowMap(table, currentSchemaID));
		LOGGER.info(String.format("bootstrapping started for %s.%s", task.database, task.table));

		try ( Connection connection = getMaxwellConnection();
			  Connection streamingConnection = getStreamingConnection(task.database)) {
			setBootstrapRowToStarted(task.id, connection);
			ResultSet resultSet = getAllRows(task.database, task.table, table, task.whereClause, streamingConnection);
			int insertedRows = 0;
			lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
			while ( resultSet.next() ) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table.database, table.name, table.getPKList());
				setRowValues(row, resultSet, table);
				row.setSchemaId(currentSchemaID);

				Scripting scripting = context.getConfig().scripting;
				if ( scripting != null )
					scripting.invoke(row);

				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
				++insertedRows;
				updateInsertedRowsColumn(insertedRows, task.id, connection);
			}
			setBootstrapRowToCompleted(insertedRows, task.id, connection);
		} catch ( NoSuchElementException e ) {
			LOGGER.info("bootstrapping aborted for " + task.logString());
		}
	}

	private void updateInsertedRowsColumn(int insertedRows, Long id, Connection connection) throws SQLException, NoSuchElementException {
		long now = System.currentTimeMillis();
		if ( now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS ) {
			String sql = "update `bootstrap` set inserted_rows = ? where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setLong(2, id);
			if ( preparedStatement.executeUpdate() == 0 ) {
				throw new NoSuchElementException();
			}
			lastInsertedRowsUpdateTimeMillis = now;
		}
	}

	protected Connection getConnection(String databaseName) throws SQLException {
		Connection conn = context.getReplicationConnection();
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getStreamingConnection(String databaseName) throws SQLException, URISyntaxException {
		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getConnectionURI(false), context.getConfig().replicationMysql.user, context.getConfig().replicationMysql.password);
		conn.setCatalog(databaseName);
		return conn;
	}

	protected Connection getMaxwellConnection() throws SQLException {
		Connection conn = context.getMaxwellConnection();
		conn.setCatalog(context.getConfig().databaseName);
		return conn;
	}

	private RowMap bootstrapStartRowMap(Table table, Long currentSchemaID) {
		RowMap row = bootstrapEventRowMap("bootstrap-start", table.database, table.name, table.getPKList());
		row.setSchemaId(currentSchemaID);
		return row;
	}

	private RowMap bootstrapEventRowMap(String type, String db, String tbl, List<String> pkList) {
		return new RowMap(
			type,
			db,
			tbl,
			System.currentTimeMillis(),
			pkList,
			null);
	}

	public void completeBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) throws Exception {
		RowMap row = bootstrapEventRowMap("bootstrap-complete", task.database, task.table, new ArrayList<>());
		row.setSchemaId(currentSchemaID);
		producer.push(row);
		LOGGER.info("bootstrapping ended for " + task.logString());
	}

	private Table findTable(String tableName, Database database) {
		Table table = database.findTable(tableName);
		if ( table == null )
			throw new RuntimeException("Couldn't find table " + tableName);
		return table;
	}

	private Database findDatabase(Schema schema, String databaseName) {
		Database database = schema.findDatabase(databaseName);
		if ( database == null )
			throw new RuntimeException("Couldn't find database " + databaseName);
		return database;
	}

	private ResultSet getAllRows(String databaseName, String tableName, Table table, String whereClause,
								 Connection connection) throws SQLException {
		Statement statement = createBatchStatement(connection);
		String pk = table.getPKString();

		String sql = String.format("select * from `%s`.%s", databaseName, tableName);

		if ( whereClause != null && !whereClause.equals("") ) {
			sql += String.format(" where %s", whereClause);
		}

		if ( pk != null && !pk.equals("") ) {
			sql += String.format(" order by %s", pk);
		}

		return statement.executeQuery(sql);
	}

	private Statement createBatchStatement(Connection connection) throws SQLException {
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	private final String startBootstrapSQL = "update `bootstrap` set started_at=NOW() where id=?";
	private void setBootstrapRowToStarted(Long id, Connection connection) throws SQLException, NoSuchElementException {
		PreparedStatement preparedStatement = connection.prepareStatement(startBootstrapSQL);
		preparedStatement.setLong(1, id);
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private final String completeBootstrapSQL = "update `bootstrap` set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
	private void setBootstrapRowToCompleted(int insertedRows, Long id, Connection connection) throws SQLException, NoSuchElementException {
		PreparedStatement preparedStatement = connection.prepareStatement(completeBootstrapSQL);
		preparedStatement.setInt(1, insertedRows);
		preparedStatement.setLong(2, id);
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			Object columnValue;

			// need to explicitly coerce TIME into TIMESTAMP in order to preserve nanoseconds
			if ( columnDefinition instanceof TimeColumnDef )
				columnValue = resultSet.getTimestamp(columnIndex);
			else
				columnValue = resultSet.getObject(columnIndex);

			row.putData(
				columnDefinition.getName(),
				columnValue == null ? null : columnDefinition.asJSON(columnValue, new MaxwellOutputConfig())
			);

			++columnIndex;
		}
	}

}
