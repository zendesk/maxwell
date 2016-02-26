package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellReplicator;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SynchronousBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);
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
	public void startBootstrap(RowMap startBootstrapRow, AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
		String databaseName = bootstrapDatabase(startBootstrapRow);
		String tableName = bootstrapTable(startBootstrapRow);

		LOGGER.debug(String.format("bootstrapping request for %s.%s", databaseName, tableName));

		Schema schema = replicator.getSchema();
		Database database = findDatabase(schema, databaseName);
		Table table = findTable(tableName, database);

		BinlogPosition position = startBootstrapRow.getPosition();
		producer.push(startBootstrapRow);
		producer.push(bootstrapStartRowMap(table, position));
		LOGGER.info(String.format("bootstrapping started for %s.%s, binlog position is %s", databaseName, tableName, position.toString()));
		try ( Connection connection = getConnection() ) {
			setBootstrapRowToStarted(startBootstrapRow, connection);
			ResultSet resultSet = getAllRows(databaseName, tableName, schema, connection);
			int insertedRows = 0;
			while ( resultSet.next() ) {
				RowMap row = new RowMap(
						"bootstrap-insert",
						databaseName,
						tableName,
						System.currentTimeMillis() / 1000,
						table.getPKList(),
						position);
				setRowValues(row, resultSet, table);

				if ( LOGGER.isDebugEnabled() )
					LOGGER.debug("bootstrapping row : " + row.toJSON());

				producer.push(row);
				++insertedRows;
				updateInsertedRowsColumn(insertedRows, startBootstrapRow, position, connection);
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
		Connection conn = context.getReplicationConnectionPool().getConnection();
		conn.setCatalog(context.getConfig().databaseName);
		return conn;
	}

	private RowMap bootstrapStartRowMap(Table table, BinlogPosition position) {
		return bootstrapEventRowMap("bootstrap-start", table, position);
	}

	private RowMap bootstrapCompleteRowMap(Table table, BinlogPosition position) {
		return bootstrapEventRowMap("bootstrap-complete", table, position);
	}

	private RowMap bootstrapEventRowMap(String type, Table table, BinlogPosition position) {
		return new RowMap(
				type,
				table.getDatabase(),
				table.getName(),
				System.currentTimeMillis() / 1000,
				table.getPKList(),
				position);
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
		String databaseName = bootstrapDatabase(completeBootstrapRow);
		String tableName = bootstrapTable(completeBootstrapRow);

		Database database = findDatabase(replicator.getSchema(), databaseName);
		ensureTable(tableName, database);
		Table table = findTable(tableName, database);

		BinlogPosition position = completeBootstrapRow.getPosition();
		producer.push(completeBootstrapRow);
		producer.push(bootstrapCompleteRowMap(table, position));

		LOGGER.info(String.format("bootstrapping ended for %s.%s", databaseName, tableName));
	}

	@Override
	public void resume(AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
		try ( Connection connection = context.getMaxwellConnectionPool().getConnection() ) {
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
	public void work(RowMap row, AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
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

	private ResultSet getAllRows(String databaseName, String tableName, Schema schema, Connection connection) throws SQLException, InterruptedException {
		Statement statement = createBatchStatement(connection);
		String pk = schema.findDatabase(databaseName).findTable(tableName).getPKString();
		if ( pk != null && !pk.equals("") ) {
			return statement.executeQuery(String.format("select * from %s.%s order by %s", databaseName, tableName, pk));
		} else {
			return statement.executeQuery(String.format("select * from %s.%s", databaseName, tableName));
		}
	}

	private Statement createBatchStatement(Connection connection) throws SQLException, InterruptedException {
		Statement statement = connection.createStatement();
		statement.setFetchSize(context.getConfig().bootstrapperBatchFetchSize);
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

			if ( columnValue != null )
				row.putData(columnDefinition.getName(), columnDefinition.asJSON(columnValue));

			++columnIndex;
		}
	}

}
