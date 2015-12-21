package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellReplicator;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;

import java.io.IOException;
import java.sql.SQLException;

public abstract class AbstractBootstrapper {

	protected MaxwellContext context;

	public AbstractBootstrapper(MaxwellContext context) { this.context = context; }

	abstract public boolean isStartBootstrapRow(RowMap row);

	abstract public boolean isCompleteBootstrapRow(RowMap row);

	abstract public boolean isBootstrapRow(RowMap row);

	abstract public boolean shouldSkip(RowMap row) throws SQLException, IOException;

	abstract public void startBootstrap(RowMap startBootstrapRow, AbstractProducer producer, MaxwellReplicator replicator) throws Exception;

	abstract public void completeBootstrap(RowMap completeBootstrapRow, AbstractProducer producer, MaxwellReplicator replicator) throws Exception;

	public abstract void resume(AbstractProducer producer, MaxwellReplicator replicator) throws Exception;

	public void work(RowMap row, AbstractProducer producer, MaxwellReplicator replicator) throws Exception {
	 	if ( isStartBootstrapRow(row) ) {
			startBootstrap(row, producer, replicator);
		} else if ( isCompleteBootstrapRow(row) ) {
			completeBootstrap(row, producer, replicator);
		}
	}
}
