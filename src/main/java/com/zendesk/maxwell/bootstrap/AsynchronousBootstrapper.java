package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.producer.AbstractProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class AsynchronousBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellReplicator.class);

	private Thread thread = null;
	private Queue<RowMap > queue = new LinkedList<>();
	private RowMap bootstrappedRow = null;
	private RowMapBufferByTable skippedRows = null;
	private SynchronousBootstrapper synchronousBootstrapper = getSynchronousBootstrapper();

	public AsynchronousBootstrapper( MaxwellContext context ) throws IOException {
		super(context);
		skippedRows = new RowMapBufferByTable();
	}

	@Override
	public boolean isStartBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isStartBootstrapRow(row);
	}

	@Override
	public boolean isCompleteBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isCompleteBootstrapRow(row);
	}

	@Override
	public boolean isBootstrapRow(RowMap row) {
		return synchronousBootstrapper.isBootstrapRow(row);
	}

	protected SynchronousBootstrapper getSynchronousBootstrapper( ) {
		return new SynchronousBootstrapper(context);
	}

	@Override
	public boolean shouldSkip(RowMap row) throws SQLException, IOException {
		// The main replication thread skips rows of the currently bootstrapped
		// table and the tables that are queued for bootstrap. The bootstrap thread replays them at
		// the end of the bootstrap. If maxwell is stopped these skipped rows will be lost;
		// however, at next startup resume() restarts the bootstrap process from the
		// beginning which restores the consistency of the replication stream.
		if ( bootstrappedRow != null && haveSameTable(row, bootstrappedRow) ) {
			skippedRows.add(row);
			return true;
		}
		for ( RowMap queuedRow : queue ) {
			if ( haveSameTable(row, queuedRow) ) {
				skippedRows.add(row);
				return true;
			}
		}
		return false;
	}

	private boolean haveSameTable(RowMap row, RowMap bootstrapStartRow) {
		String databaseName = ( String ) bootstrapStartRow.getData("database_name");
		String tableName = ( String ) bootstrapStartRow.getData("table_name");
		return row.getDatabase().equals(databaseName) && row.getTable().equals(tableName);
	}

	@Override
	public void startBootstrap(final RowMap bootstrapStartRow, final AbstractProducer producer, final MaxwellReplicator replicator) throws Exception {
		if (thread == null) {
			thread = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						synchronousBootstrapper.startBootstrap(bootstrapStartRow, producer, replicator);
					} catch ( Exception e ) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			});
			thread.start();
		} else {
			queueRow(bootstrapStartRow);
		}
	}

	private void queueRow(RowMap row) {
		String databaseName = ( String ) row.getData("database_name");
		String tableName = ( String ) row.getData("table_name");
		queue.add(row);
		LOGGER.info(String.format("async bootstrapping: queued table %s.%s for bootstrapping", databaseName, tableName));
	}

	@Override
	public void completeBootstrap(RowMap bootstrapCompleteRow, AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
		String databaseName = ( String ) bootstrapCompleteRow.getData("database_name");
		String tableName = ( String ) bootstrapCompleteRow.getData("table_name");
		try {
			replaySkippedRows(databaseName, tableName, producer, bootstrapCompleteRow);
			synchronousBootstrapper.completeBootstrap(bootstrapCompleteRow, producer, replicator);
			LOGGER.info(String.format("async bootstrapping ended for %s.%s", databaseName, tableName));
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			thread = null;
			bootstrappedRow = null;
		}
		if ( !queue.isEmpty() ) {
			startBootstrap(queue.remove(), producer, replicator);
		}
	}

	private void replaySkippedRows(String databaseName, String tableName, AbstractProducer producer, RowMap bootstrapCompleteRow) throws Exception {
		BinlogPosition bootstrapStartBinlogPosition = getBootstrapStartBinlogPosition(bootstrapCompleteRow);
		LOGGER.info("async bootstrapping: replaying " + skippedRows.size(databaseName, tableName) + " skipped rows...");
		skippedRows.flushToDisk(databaseName, tableName);
		while ( skippedRows.size(databaseName, tableName) > 0 ) {
			RowMap row = skippedRows.removeFirst(databaseName, tableName);
			if ( row.getPosition().newerThan(bootstrapStartBinlogPosition) )
				producer.push(row);
		}
		LOGGER.info("async bootstrapping: replay complete");
	}

	private BinlogPosition getBootstrapStartBinlogPosition(RowMap bootstrapCompleteRow) throws SQLException {
		try ( Connection connection = context.getConnectionPool().getConnection() ) {
			String sql = "select * from maxwell.bootstrap where id = ?";
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setLong(1, ( Long ) bootstrapCompleteRow.getData("id"));
			ResultSet resultSet = preparedStatement.executeQuery();
			resultSet.next();
			return new BinlogPosition(resultSet.getLong("binlog_position"), resultSet.getString("binlog_file"));
		}
	}

	@Override
	public void resume(AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
		synchronousBootstrapper.resume(producer, replicator);
	}

	public void join() throws InterruptedException {
		if ( thread != null ) {
			thread.join();
		}
	}
}

