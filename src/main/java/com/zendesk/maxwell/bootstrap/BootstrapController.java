package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapBuffer;
import com.zendesk.maxwell.util.ConnectionPool;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BootstrapController extends RunLoopProcess  {
	static final Logger LOGGER = LoggerFactory.getLogger(BootstrapController.class);
	private final long MAX_TX_ELEMENTS = 10000;

	private final ConnectionPool maxwellConnectionPool;
	private final SynchronousBootstrapper bootstrapper;
	private final AbstractProducer producer;
	private final String clientID;
	private final boolean syncMode;
	private Long currentSchemaID;

	public BootstrapController(
		ConnectionPool maxwellConnectionPool,
		AbstractProducer producer,
		SynchronousBootstrapper bootstrapper,
		String clientID,
		boolean syncMode,
		Long currentSchemaID
	) {
		this.maxwellConnectionPool = maxwellConnectionPool;
		this.producer = producer;
		this.bootstrapper = bootstrapper;
		this.clientID = clientID;
		this.syncMode = syncMode;
		this.currentSchemaID = currentSchemaID;
	}

	// this mutex is used to block rows from being produced while a "synchronous"
	// bootstrap is run
	private Object bootstrapMutex = new Object();

	// this one is used to protect against races in an async producer.
	private Object completionMutex = new Object();
	private BootstrapTask activeTask;
	private RowMapBuffer skippedRows = new RowMapBuffer(MAX_TX_ELEMENTS);

	@Override
	protected void work() throws Exception {
		try {
			doWork();
		} catch ( InterruptedException e ) {
		} catch ( SQLException e ) {
			LOGGER.error("got SQLException trying to bootstrap", e);
		}
	}

	private void doWork() throws Exception {
		List<BootstrapTask> tasks = getIncompleteTasks();
		synchronized(bootstrapMutex) {
			for ( BootstrapTask task : tasks ) {
				LOGGER.debug("starting bootstrap task: {}", task.logString());
				synchronized(completionMutex) {
					activeTask = task;
				}

				bootstrapper.startBootstrap(task, producer, getCurrentSchemaID());

				synchronized(completionMutex) {
					pushSkippedRows();
					activeTask = null;
				}
			}
		}

		Thread.sleep(1000);
	}

	private synchronized Long getCurrentSchemaID() {
		return this.currentSchemaID;
	}

	public synchronized void setCurrentSchemaID(long schemaID) {
		this.currentSchemaID = schemaID;
	}

	private List<BootstrapTask> getIncompleteTasks() throws SQLException {
		ArrayList<BootstrapTask> list = new ArrayList<>();
		try ( Connection cx = maxwellConnectionPool.getConnection() ) {
			PreparedStatement s = cx.prepareStatement("select * from bootstrap where is_complete = 0 and client_id = ? and (started_at is null or started_at <= now()) order by isnull(started_at), started_at asc, id asc");
			s.setString(1, this.clientID);

			ResultSet rs = s.executeQuery();

			while (rs.next()) {
				list.add(BootstrapTask.valueOf(rs));
			}
		}
		return list;
	}

	public boolean shouldSkip(RowMap row) throws IOException {
		// The main replication thread skips rows of the currently bootstrapped
		// table and the tables that are queued for bootstrap. The bootstrap thread replays them at
		// the end of the bootstrap.

		if ( syncMode )
			synchronized(bootstrapMutex) { return false; }
		else {
			synchronized (completionMutex) {
				if (activeTask == null)
					return false;

				// async mode with an active task
				if (activeTask.matches(row)) {
					skippedRows.add(row);
					return true;
				} else
					return false;
			}
		}
	}

	private void pushSkippedRows() throws Exception {
		skippedRows.flushToDisk();
		while ( skippedRows.size() > 0 ) {
			RowMap row = skippedRows.removeFirst();
			producer.push(row);
		}
	}

}
