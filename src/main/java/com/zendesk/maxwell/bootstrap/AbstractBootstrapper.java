package com.zendesk.maxwell.bootstrap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingDeque;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.BootstrapRowMap;
import com.zendesk.maxwell.row.RowMap;


public abstract class AbstractBootstrapper {

	protected MaxwellContext context;
	private final LinkedBlockingDeque<BootstrapRowMap> bootstrapQueue = new LinkedBlockingDeque<>(20);
	private BootstrapPoller bootstrapPoller = null;

	public AbstractBootstrapper(MaxwellContext context) {
		this.context = context;
		this.bootstrapPoller = new BootstrapPoller(bootstrapQueue, context);
		context.addTask(bootstrapPoller);
	}

	public boolean isStartBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") == null &&
			row.getData("completed_at") == null &&
			( long ) row.getData("is_complete") == 0;
	}

	public boolean isCompleteBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") != null &&
			row.getData("completed_at") != null &&
			( long ) row.getData("is_complete") == 1;
	}

	public boolean isBootstrapRow(RowMap row) {
		return row instanceof BootstrapRowMap;
	}

	protected String bootstrapDatabase(RowMap rowmap) {
		return (String) rowmap.getData("database_name");
	}

	protected String bootstrapTable(RowMap rowmap) {
		return (String) rowmap.getData("table_name");
	}

	protected String bootstrapWhere(RowMap rowmap) {
		return (String) rowmap.getData("where_clause");
	}

	public BootstrapRowMap pollBootstrapEvent() {
		bootstrapPoller.ensureBootstrapPoller();

		if (bootstrapQueue.isEmpty()) {
			return null;
		}

		return bootstrapQueue.removeFirst();
	}

	abstract public boolean shouldSkip(RowMap row) throws SQLException, IOException;

	abstract public void startBootstrap(RowMap startBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception;

	abstract public void completeBootstrap(RowMap completeBootstrapRow, AbstractProducer producer, Replicator replicator) throws Exception;

	public abstract void resume(AbstractProducer producer, Replicator replicator) throws Exception;

	public abstract boolean isRunning();

	public abstract void work(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception;
}
