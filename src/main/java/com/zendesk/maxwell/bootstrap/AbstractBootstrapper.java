package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.sql.SQLException;

public abstract class AbstractBootstrapper {
	protected MaxwellContext context;

	protected AbstractBootstrapper(MaxwellContext context) {
		this.context = context;
	}

	protected boolean isStartBootstrapRow(RowMap row) {
		return isBootstrapRow(row) &&
			row.getData("started_at") == null &&
			row.getData("completed_at") == null &&
			( long ) row.getData("is_complete") == 0;
	}

	protected boolean isBootstrapRow(RowMap row) {
		return row.getDatabase().equals(this.context.getConfig().databaseName) &&
			row.getTable().equals("bootstrap") &&
			row.getData("client_id").equals(this.context.getConfig().clientID);
	}

	abstract public boolean shouldSkip(RowMap row) throws SQLException, IOException;

	public abstract void resume(AbstractProducer producer) throws SQLException;

	public abstract boolean isRunning();

	public abstract void work(RowMap row, AbstractProducer producer, Long currentSchemaID) throws Exception;
}
