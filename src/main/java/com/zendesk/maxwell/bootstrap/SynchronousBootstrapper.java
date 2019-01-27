package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.TimeColumnDef;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SynchronousBootstrapper extends AbstractBootstrapper {
	static final Logger LOGGER = LoggerFactory.getLogger(SynchronousBootstrapper.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;

	private long lastInsertedRowsUpdateTimeMillis = 0;

	public SynchronousBootstrapper(MaxwellContext context) { super(context); }

	@Override
	public boolean shouldSkip(RowMap row) {
		// the synchronous bootstrapper blocks other incoming messages
		// to the replication stream so there's nothing to skip
		return false;
	}

	public void startBootstrap(BootstrapTask task, AbstractProducer producer, Replicator replicator) throws Exception {
		performBootstrap(task, producer, replicator);
		completeBootstrap(task, producer, replicator);
	}

	public void performBootstrap(BootstrapTask task, AbstractProducer producer, Replicator replicator) throws Exception {
		LOGGER.debug("bootstrapping requested for " + task.logString());

		Schema schema = replicator.getSchema();
		Database database = findDatabase(schema, task.database);
		Table table = findTable(task.table, database);

		Long schemaId = replicator.getSchemaId();
		producer.push(bootstrapStartRowMap(table));
		LOGGER.info(String.format("bootstrapping started for %s.%s", task.database, task.table));

		try ( Connection connection = getConnection();
			  Connection streamingConnection = getStreamingConnection()) {
			setBootstrapRowToStarted(task.id, connection);
			ResultSet resultSet = getAllRows(task.database, task.table, table, task.whereClause, streamingConnection);
			int insertedRows = 0;
			lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
			while ( resultSet.next() ) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table);
				setRowValues(row, resultSet, table);
				row.setSchemaId(schemaId);

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

	protected Connection getConnection() throws SQLException {
		Connection conn = context.getReplicationConnection();
		conn.setCatalog(context.getConfig().databaseName);
		return conn;
	}

	protected Connection getStreamingConnection() throws SQLException, URISyntaxException {
		Connection conn = DriverManager.getConnection(context.getConfig().replicationMysql.getConnectionURI(), context.getConfig().replicationMysql.user, context.getConfig().replicationMysql.password);
		conn.setCatalog(context.getConfig().databaseName);
		return conn;
	}

	private RowMap bootstrapStartRowMap(Table table) {
		return bootstrapEventRowMap("bootstrap-start", table);
	}

	private RowMap bootstrapCompleteRowMap(Table table) {
		return bootstrapEventRowMap("bootstrap-complete", table);
	}

	private RowMap bootstrapEventRowMap(String type, Table table) {
		return new RowMap(
				type,
				table.getDatabase(),
				table.getName(),
				System.currentTimeMillis(),
				table.getPKList(),
				null);
	}

	public void completeBootstrap(BootstrapTask task, AbstractProducer producer, Replicator replicator) throws Exception {
		Database database = findDatabase(replicator.getSchema(), task.database);
		ensureTable(task.table, database);
		Table table = findTable(task.table, database);

		producer.push(bootstrapCompleteRowMap(table));

		LOGGER.info("bootstrapping ended for " + task.logString());
	}

	@Override
	public void resume(AbstractProducer producer, Replicator replicator) throws SQLException {
		try (Connection connection = context.getMaxwellConnection()) {
			// This update resets all rows of incomplete bootstraps to their original state.
			// These updates are treated as fresh bootstrap requests and trigger a restart
			// of the bootstrap process from the beginning.
			String clientID = this.context.getConfig().clientID;
			String sql = "update `bootstrap` set started_at = NULL where is_complete = 0 and started_at is not NULL and client_id = ?";
			PreparedStatement s = connection.prepareStatement(sql);
			s.setString(1, clientID);
			s.execute();
		}
	}

	@Override
	public boolean isRunning( ) {
		return false;
	}

	@Override
	public void work(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception {
		try {
			if ( isStartBootstrapRow(row) ) {
				producer.push(row);
				startBootstrap(BootstrapTask.valueOf(row), producer, replicator);
			}
		} catch ( NoSuchElementException e ) {
			LOGGER.info(String.format("bootstrapping cancelled for %s.%s", row.getDatabase(), row.getTable()));
		}
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

	private void ensureTable(String tableName, Database database) {
		findTable(tableName, database);
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
