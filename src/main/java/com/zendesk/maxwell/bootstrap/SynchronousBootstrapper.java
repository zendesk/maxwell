package com.zendesk.maxwell.bootstrap;

import com.google.code.or.OpenReplicator;
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

public class SynchronousBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);
	private static final long INSERTED_ROWS_UPDATE_PERIOD_MILLIS = 250;

	private long lastInsertedRowsUpdateTimeMillis = 0;

	public SynchronousBootstrapper(MaxwellContext context) { super(context); }

	@Override
	public boolean isStartBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") == null &&
			row.getData("completed_at") == null &&
			( long ) row.getData("is_complete") == 0;
	}

	@Override
	public boolean isCompleteBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") != null &&
			row.getData("completed_at") != null &&
			( long ) row.getData("is_complete") == 1;
	}

	@Override
	public boolean isBootstrapRow(RowMap row) {
		return row.getDatabase().equals("maxwell") &&
			row.getTable().equals("bootstrap");
	}

	@Override
	public boolean shouldSkip(RowMap row) {
		// the synchronous bootstrapper blocks other incoming messages
		// to the replication stream so there's nothing to skip
		return false;
	}

	@Override
	public void startBootstrap(RowMap startBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		String databaseName = ( String ) startBootstrapRow.getData("database_name");
		String tableName = ( String ) startBootstrapRow.getData("table_name");
		LOGGER.debug(String.format("bootstrapping request for %s.%s", databaseName, tableName));
		Database database = findDatabase(schema, databaseName);
		Table table = findTable(tableName, database);
		BinlogPosition position = new BinlogPosition(replicator.getBinlogPosition(), replicator.getBinlogFileName());
		producer.push(startBootstrapRow);
		producer.push(bootstrapStartRowMap(table, position));
		LOGGER.info(String.format("bootstrapping started for %s.%s, binlog position is %s", databaseName, tableName, position.toString()));
		try ( Connection connection = getConnection() ) {
			setBootstrapRowToStarted(startBootstrapRow, connection);
			ResultSet resultSet = getAllRows(databaseName, tableName, connection);
			int insertedRows = 0;
			while ( resultSet.next() ) {
				RowMap row = new RowMap(
						"insert",
						databaseName,
						tableName,
						System.currentTimeMillis(),
						table.getPKList(),
						position);
				setRowValues(row, resultSet, table);
				LOGGER.debug("bootstrapping row : " + row.toJSON());
				producer.push(row);
				++insertedRows;
				updateInsertedRowsColumn(insertedRows, startBootstrapRow, connection);
			}
			setBootstrapRowToCompleted(insertedRows, startBootstrapRow, connection);
		}
	}

	private void updateInsertedRowsColumn(int insertedRows, RowMap startBootstrapRow, Connection connection) throws SQLException {
		long now = System.currentTimeMillis();
		if ( now - lastInsertedRowsUpdateTimeMillis > INSERTED_ROWS_UPDATE_PERIOD_MILLIS ) {
			long rowId = ( long ) startBootstrapRow.getData("id");
			String sql = "update maxwell.bootstrap set inserted_rows = ? where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, insertedRows);
			preparedStatement.setLong(2, rowId);
			preparedStatement.execute();
			lastInsertedRowsUpdateTimeMillis = now;
		}
	}

	protected Connection getConnection( ) throws SQLException {
		return context.getConnectionPool().getConnection();
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
				table.getDatabase().getName(),
				table.getName(),
				System.currentTimeMillis(),
				table.getPKList(),
				position);
	}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		String databaseName = ( String ) completeBootstrapRow.getData("database_name");
		String tableName = ( String ) completeBootstrapRow.getData("table_name");
		Database database = findDatabase(schema, databaseName);
		ensureTable(tableName, findDatabase(schema, databaseName));
		Table table = findTable(tableName, database);
		BinlogPosition position = new BinlogPosition(replicator.getBinlogPosition(), replicator.getBinlogFileName());
		producer.push(completeBootstrapRow);
		producer.push(bootstrapCompleteRowMap(table, position));
		LOGGER.info(String.format("bootstrapping ended for %s.%s", databaseName, tableName));
	}

	@Override
	public void resume(Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
		try ( Connection connection = context.getConnectionPool().getConnection() ) {
			// This update resets all rows of incomplete bootstraps to their original state.
			// These updates are treated as fresh bootstrap requests and trigger a restart
			// of the bootstrap process from the beginning.
			String sql = "update maxwell.bootstrap set started_at = NULL where is_complete = 0 and started_at is not NULL";
			connection.prepareStatement(sql).execute();
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

	private ResultSet getAllRows(String databaseName, String tableName, Connection connection) throws SQLException, InterruptedException {
		Statement statement = createBatchStatement(connection);
		return statement.executeQuery(String.format("select * from %s.%s", databaseName, tableName));
	}

	private Statement createBatchStatement(Connection connection) throws SQLException, InterruptedException {
		Statement statement = connection.createStatement();
		statement.setFetchSize(context.getConfig().bootstrapperBatchFetchSize);
		return statement;
	}

	private void setBootstrapRowToStarted(RowMap startBootstrapRow, Connection connection) throws SQLException {
		String sql = "update maxwell.bootstrap set started_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, ( Long ) startBootstrapRow.getData("id"));
		preparedStatement.execute();
	}

	private void setBootstrapRowToCompleted(int insertedRows, RowMap startBootstrapRow, Connection connection) throws SQLException {
		String sql = "update maxwell.bootstrap set is_complete=1, inserted_rows=?, completed_at=NOW() where id=?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setInt(1, insertedRows);
		preparedStatement.setLong(2, ( Long ) startBootstrapRow.getData("id"));
		preparedStatement.execute();
	}

	private void setRowValues(RowMap row, ResultSet resultSet, Table table) throws SQLException, IOException {
		Iterator<ColumnDef> columnDefinitions = table.getColumnList().iterator();
		int columnIndex = 1;
		while ( columnDefinitions.hasNext() ) {
			ColumnDef columnDefinition = columnDefinitions.next();
			row.putData(columnDefinition.getName(), columnDefinition.asJSON(resultSet.getObject(columnIndex)));
			++columnIndex;
		}
	}

}
