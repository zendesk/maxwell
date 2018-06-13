package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.row.RowMapFactory;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.replication.Replicator;

public abstract class AbstractBootstrapper implements Bootstrapper {

	protected final MaxwellContext context;
	protected final RowMapFactory rowMapFactory;

	public AbstractBootstrapper(MaxwellContext context, RowMapFactory rowMapFactory) { this.context = context;
		this.rowMapFactory = rowMapFactory;
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
		return row.getDatabase().equals(this.context.getConfig().getDatabaseName()) &&
			row.getTable().equals("bootstrap") &&
			row.getData("client_id").equals(this.context.getConfig().getClientID());
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

	abstract public void startBootstrap(RowMap startBootstrapRow, Producer producer, Replicator replicator) throws Exception;

	abstract public void completeBootstrap(RowMap completeBootstrapRow, Producer producer, Replicator replicator) throws Exception;

}
