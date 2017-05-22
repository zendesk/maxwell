package com.zendesk.maxwell.bootstrap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.BootstrapRowMap;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.StoppableTaskState;

public class BootstrapPoller implements StoppableTask {
	static final Logger LOGGER = LoggerFactory.getLogger(BootstrapPoller.class);

	protected Connection schemaConnection;
	protected volatile StoppableTaskState taskState;
	private long pollInterval = 1000L;
	private String sql = null;
	private MaxwellContext context = null;
	private String schemaDatabase = null;
	private Thread bootstrapPollerThread = null;
	private final LinkedBlockingDeque<BootstrapRowMap> queue;
	private Map<Integer, BootstrapEntry> allentry = new HashMap<Integer, BootstrapEntry>();

	public static class BootstrapEntry {
		public int id;
		public String database;
		public String table;
		public String where;
		public boolean complete;
		public long inserted_rows;
		public long total_rows;
		public Timestamp created_at;
		public Timestamp started_at;
		public Timestamp completed_at;
		public String binlog_file;
		public int binlog_position;
	}

	public BootstrapPoller(LinkedBlockingDeque<BootstrapRowMap> queue, MaxwellContext context) {
		this.queue = queue;
		this.context = context;
		this.schemaDatabase = context.getConfig().databaseName;
		this.sql = "select * from bootstrap;";
		this.taskState = new StoppableTaskState(this.getClass().getName());
		this.pollInterval = context.getConfig().bootstrapPollerInterval;
	}

	protected void ensureBootstrapPoller() {
		if (bootstrapPollerThread != null && bootstrapPollerThread.isAlive()) {
			return;
		}

		if (taskState.isRunning() == false) {
			// stopped by request. Do not restart again.
			return;
		}

		bootstrapPollerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				poll();
			}
		});
		bootstrapPollerThread.start();
	}

	private void poll() {
		try {
			if (schemaConnection == null) {
				schemaConnection = this.context.getMaxwellConnection();
				schemaConnection.setCatalog(context.getConfig().databaseName);
				// ensure to get latest data.
				schemaConnection.setAutoCommit(true);
			}

			while (this.taskState.isRunning()) {
				Statement statement = schemaConnection.createStatement();
				ResultSet rs = statement.executeQuery(this.sql);

				while (rs.next()) {
					BootstrapEntry entry = new BootstrapEntry();

					entry.id = rs.getInt(1);
					entry.database = rs.getString(2);
					entry.table = rs.getString(3);
					entry.where = rs.getString(4);
					entry.complete = rs.getBoolean(5);
					entry.inserted_rows = rs.getLong(6);
					entry.total_rows = rs.getLong(7);
					entry.created_at = rs.getTimestamp(8);
					entry.started_at = rs.getTimestamp(9);
					entry.completed_at = rs.getTimestamp(10);
					entry.binlog_file = rs.getString(11);
					entry.binlog_position = rs.getInt(12);

					checkEntry(entry);
				}

				rs.close();
				statement.close();

				Thread.sleep(pollInterval);
			}
		} catch (InterruptedException e) {
			// ignored
		} catch (Exception e) {
			LOGGER.error(String.format("Bootstrap poller exited on error: %s", e.toString()));
		} finally {
			if (schemaConnection != null) {
				try {
					schemaConnection.close();
				} catch (SQLException e) {
					// ignored
				}
			}
			schemaConnection = null;
		}
	}

	private void checkEntry(BootstrapEntry entry) throws IOException, SQLException {
		BootstrapEntry old = allentry.get(entry.id);
		if (old == null && entry.started_at == null && entry.complete == false) {
			// new inserted row
			BootstrapRowMap row = new BootstrapRowMap("insert", this.schemaDatabase, entry, context.getPosition());
			LOGGER.debug(String.format("found a new bootstrapping row %s", row.toJSON()));
			queue.push(row);
		} else if (old != null && old.completed_at == null && old.complete == false && entry.completed_at != null
				&& entry.complete == true) {
			// completed row
			BootstrapRowMap row = new BootstrapRowMap("update", this.schemaDatabase, entry, context.getPosition());
			LOGGER.debug(String.format("found a updated bootstrapping row %s", row.toJSON()));
			queue.push(row);
		} else if (old != null && old.started_at != null && old.complete == false && entry.started_at == null
				&& entry.complete == false) {
			// resume
			BootstrapRowMap row = new BootstrapRowMap("update", this.schemaDatabase, entry, context.getPosition());
			LOGGER.debug(String.format("found a resumed bootstrapping row %s", row.toJSON()));
			queue.push(row);
		}

		allentry.put(entry.id, entry);
	}

	@Override
	public void requestStop() {
		this.taskState.requestStop();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		this.taskState.awaitStop(bootstrapPollerThread, timeout);
	}
}
