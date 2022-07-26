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

/**
 * Watches maxwell.bootstrap, starts and stops bootstrap tasks
 */
public class BootstrapController extends RunLoopProcess  {
	static final Logger LOGGER = LoggerFactory.getLogger(BootstrapController.class);
	private final long MAX_TX_ELEMENTS = 10000;

	private final ConnectionPool maxwellConnectionPool;
	private final SynchronousBootstrapper bootstrapper;
	private final AbstractProducer producer;
	private final String clientID;
	private final boolean syncMode;
	private Long currentSchemaID;

	/**
	 * Instantiate a controller
	 * @param maxwellConnectionPool maxwell connection pool
	 * @param producer where to write rows
	 * @param bootstrapper the "actor" that actually does work
	 * @param clientID current client ID
	 * @param syncMode whether to stop replication while we bootstrap
	 * @param currentSchemaID initial value for schema_id
	 */
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
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("starting bootstrap task: {}", task.logString());
				}
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

	/**
	 * setup a value for outputting as "schema_id".
	 *
	 * Note that this is laughably unreliable, as there's totally no way of
	 * syncing the bootstrap's work with the replicators'.  But one of my great
	 * talents as an engineer has been ignoring stuff that's sublty wrong but
	 * functionally useful.
	 * @param schemaID the totally disconnected from reality schema_id
	 */
	public synchronized void setCurrentSchemaID(Long schemaID) {
		this.currentSchemaID = schemaID;
	}

	private List<BootstrapTask> getIncompleteTasks() throws SQLException {
		ArrayList<BootstrapTask> list = new ArrayList<>();
		try ( Connection cx = maxwellConnectionPool.getConnection();
			  PreparedStatement s = cx.prepareStatement("select * from bootstrap where is_complete = 0 and client_id = ? and (started_at is null or started_at <= now()) order by isnull(started_at), started_at asc, id asc") ) {
			s.setString(1, this.clientID);

			try ( ResultSet rs = s.executeQuery() ) {
				while (rs.next()) {
					list.add(BootstrapTask.valueOf(rs));
				}
			}
		}
		return list;
	}

	/**
	 * If a bootstrap is active for a table, buffer the row for later.
	 *
	 * At the end of a bootstrap we will output the buffered rows.
	 * This allows us to output a consistant snapshot of table, first
	 * doing the SELECT * and then outputting deltas.
	 * @param row a row to possibly buffer
	 * @return whether the row was buffered
	 * @throws IOException if there was a problem buffering the row
	 */
	public boolean shouldSkip(RowMap row) throws IOException {
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
