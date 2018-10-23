package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.scripting.Scripting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

	@Override
	public void startBootstrap(RowMap startBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {
		String databaseName = bootstrapDatabase(startBootstrapRow);
		String tableName = bootstrapTable(startBootstrapRow);

		String whereClause = bootstrapWhere(startBootstrapRow);

		String logString = String.format("bootstrapping request for %s.%s", databaseName, tableName);
		if ( whereClause != null ) {
			logString += String.format(" with where clause %s", whereClause);
		}
		LOGGER.debug(logString);

		Schema schema = replicator.getSchema();
		Database database = findDatabase(schema, databaseName);
		Table table = findTable(tableName, database);

		Position position = startBootstrapRow.getPosition();
		producer.push(startBootstrapRow);
		producer.push(bootstrapStartRowMap(table, position));
		LOGGER.info(String.format("bootstrapping started for %s.%s, binlog position is %s", databaseName, tableName, position.toString()));
		try ( Connection connection = getConnection();
			  Connection streamingConnection = getStreamingConnection()) {
			setBootstrapRowToStarted(startBootstrapRow, connection);
			ResultSet resultSet = getAllRows(databaseName, tableName, schema, whereClause, streamingConnection);
			int insertedRows = 0;
			lastInsertedRowsUpdateTimeMillis = 0; // ensure updateInsertedRowsColumn is called at least once
			while ( resultSet.next() ) {
				RowMap row = bootstrapEventRowMap("bootstrap-insert", table, position);
				setRowValues(row, resultSet, table);

				Scripting scripting = context.getConfig().scripting;
				if ( scripting != null )
					scripting.invoke(row);

				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
				++insertedRows;
				updateInsertedRowsColumn(insertedRows, startBootstrapRow, position.getBinlogPosition(), connection);
			}
			setBootstrapRowToCompleted(insertedRows, startBootstrapRow, connection);
		}
	}

	private void updateInsertedRowsColumn(int insertedRows, RowMap startBootstrapRow, BinlogPosition position, Connection connection) throws SQLException, NoSuchElementException {
		long now = System.currentTimeMillis();
		if ( now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS ) {
			long rowId = ( long ) startBootstrapRow.getData("id");
			String sql = "update `bootstrap` set inserted_rows = ?, binlog_file = ?, binlog_position = ? where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setString(2, position.getFile());
			preparedStatement.setLong(3, position.getOffset());
			preparedStatement.setLong(4, rowId);
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

	private RowMap bootstrapStartRowMap(Table table, Position position) {
		return bootstrapEventRowMap("bootstrap-start", table, position);
	}

	private RowMap bootstrapCompleteRowMap(Table table, Position position) {
		return bootstrapEventRowMap("bootstrap-complete", table, position);
	}

	private RowMap bootstrapEventRowMap(String type, Table table, Position position) {
		return new RowMap(
				type,
				table.getDatabase(),
				table.getName(),
				System.currentTimeMillis(),
				table.getPKList(),
				position);
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception {
		String databaseName = bootstrapDatabase(completeBootstrapRow);
		String tableName = bootstrapTable(completeBootstrapRow);

		Database database = findDatabase(replicator.getSchema(), databaseName);
		ensureTable(tableName, database);
		Table table = findTable(tableName, database);

		Position position = completeBootstrapRow.getPosition();
		producer.push(completeBootstrapRow);
		producer.push(bootstrapCompleteRowMap(table, position));

		LOGGER.info(String.format("bootstrapping ended for %s.%s", databaseName, tableName));
	}

	@Override
	public void resume(AbstractProducer producer, Replicator replicator) throws Exception {
		try ( Connection connection = context.getMaxwellConnection() ) {
			// This update resets all rows of incomplete bootstraps to their original state.
			// These updates are treated as fresh bootstrap requests and trigger a restart
			// of the bootstrap process from the beginning.
			String sql = "update `bootstrap` set started_at = NULL where is_complete = 0 and started_at is not NULL";
			connection.prepareStatement(sql).execute();
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
				startBootstrap(row, producer, replicator);
			} else if ( isCompleteBootstrapRow(row) ) {
				completeBootstrap(row, producer, replicator);
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

	private ResultSet getAllRows(String databaseName, String tableName, Schema schema, String whereClause,
								Connection connection) throws SQLException, InterruptedException {
		Statement statement = createBatchStatement(connection);
		String pk = schema.findDatabase(databaseName).findTable(tableName).getPKString();

		String sql = String.format("select * from `%s`.%s", databaseName, tableName);

		if ( whereClause != null && !whereClause.equals("") ) {
			sql += String.format(" where %s", whereClause);
		}

		if ( pk != null && !pk.equals("") ) {
			sql += String.format(" order by %s", pk);
		}

		return statement.executeQuery(sql);
	}

	private Statement createBatchStatement(Connection connection) throws SQLException, InterruptedException {
		Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setFetchSize(Integer.MIN_VALUE);
		return statement;
	}

	private void setBootstrapRowToStarted(RowMap startBootstrapRow, Connection connection) throws SQLException, NoSuchElementException {
		String sql = "update `bootstrap` set started_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, ( Long ) startBootstrapRow.getData("id"));
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private void setBootstrapRowToCompleted(int insertedRows, RowMap startBootstrapRow, Connection connection) throws SQLException, NoSuchElementException {
		String sql = "update `bootstrap` set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setInt(1, insertedRows);
		preparedStatement.setLong(2, ( Long ) startBootstrapRow.getData("id"));
		if ( preparedStatement.executeUpdate() == 0) {
			throw new NoSuchElementException();
		}
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException, IOException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			Object columnValue = resultSet.getObject(columnIndex);

			row.putData(
				columnDefinition.getName(),
				columnValue == null ? null : columnDefinition.asJSON(columnValue)
			);

			++columnIndex;
		}
	}

}
