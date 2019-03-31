package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBufferByTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class AsynchronousBootstrapper extends AbstractBootstrapper {
	static final Logger LOGGER = LoggerFactory.getLogger(AsynchronousBootstrapper.class);

	private Thread thread = null;
	private Queue<BootstrapTask> queue = new LinkedList<>();
	private BootstrapTask activeTask = null;

	private RowMapBufferByTable skippedRows = null;
	private SynchronousBootstrapper synchronousBootstrapper = getSynchronousBootstrapper();

	public AsynchronousBootstrapper( MaxwellContext context ) {
		super(context);
		skippedRows = new RowMapBufferByTable();
	}

	protected SynchronousBootstrapper getSynchronousBootstrapper( ) {
		return new SynchronousBootstrapper(context);
	}

	@Override
	public boolean shouldSkip(RowMap row) throws IOException {
		// The main replication thread skips rows of the currently bootstrapped
		// table and the tables that are queued for bootstrap. The bootstrap thread replays them at
		// the end of the bootstrap. If maxwell is stopped these skipped rows will be lost;
		// however, at next startup resume() restarts the bootstrap process from the
		// beginning which restores the consistency of the replication stream.
		if ( activeTask != null && haveSameTable(row, activeTask) ) {
			skippedRows.add(row);
			return true;
		}
		for ( BootstrapTask task : queue ) {
			if ( haveSameTable(row, task) ) {
				skippedRows.add(row);
				return true;
			}
		}
		return false;
	}

	private boolean haveSameTable(RowMap row, BootstrapTask activeTask) {
		return row.getDatabase().equals(activeTask.database) && row.getTable().equals(activeTask.table);
	}

	public void startBootstrap(final BootstrapTask task, final AbstractProducer producer, Long currentSchemaID) {
		if (thread == null) {
			activeTask = task;
			thread = new Thread(() -> {
				try {
					synchronousBootstrapper.performBootstrap(task, producer, currentSchemaID);
				} catch ( Exception e ) {
					e.printStackTrace();
					System.exit(1);
				} finally {
					activeTask = null;
					completeBootstrap(task, producer, currentSchemaID);
				}
			});
			thread.start();
		} else {
			queueTask(task);
		}
	}

	private void queueTask(BootstrapTask task) {
		queue.add(task);
		LOGGER.info(String.format("async bootstrapping: queued table %s.%s for bootstrapping", task.database, task.table));
	}

	private void completeBootstrap(BootstrapTask task, AbstractProducer producer, Long currentSchemaID) {
		try {
			replaySkippedRows(task, producer);
			synchronousBootstrapper.completeBootstrap(task, producer);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			thread = null;
		}
		if ( !queue.isEmpty() ) {
			startBootstrap(queue.remove(), producer, currentSchemaID);
		}
	}


	private void replaySkippedRows(BootstrapTask task, AbstractProducer producer) throws Exception {
		if ( skippedRows.size(task.database, task.table) == 0 )
			return;

		LOGGER.info("async bootstrapping: replaying " + skippedRows.size(task.database, task.table) + " skipped rows...");
		skippedRows.flushToDisk(task.database, task.table);
		while ( skippedRows.size(task.database, task.table) > 0 ) {
			RowMap row = skippedRows.removeFirst(task.database, task.table);
			producer.push(row);
		}
		LOGGER.info("async bootstrapping: replay complete");
	}


	@Override
	public void resume(AbstractProducer producer) throws SQLException {
		synchronousBootstrapper.resume(producer);
	}

	@Override
	public void work(RowMap row, AbstractProducer producer, Long currentSchemaID) {
		if ( isStartBootstrapRow(row) ) {
			startBootstrap(BootstrapTask.valueOf(row), producer, currentSchemaID);
		}
	}

	@Override
	public boolean isRunning() {
		return thread != null || queue.size() > 0;
	}
}

