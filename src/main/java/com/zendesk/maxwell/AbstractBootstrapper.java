package com.zendesk.maxwell;

import com.google.code.or.OpenReplicator;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;

import java.io.IOException;
import java.sql.SQLException;

public abstract class AbstractBootstrapper {

	protected MaxwellContext context;

	public AbstractBootstrapper(MaxwellContext context) { this.context = context; }

	abstract public boolean isStartBootstrapRow(RowMap row);

	abstract public boolean isCompleteBootstrapRow(RowMap row);

	abstract public boolean isBootstrapRow(RowMap row);

	abstract public boolean shouldSkip(RowMap row) throws SQLException, IOException;

	abstract public void startBootstrap(RowMap startBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception;

	abstract public void completeBootstrap(RowMap completeBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception;

	public abstract void resume(Schema schema, AbstractProducer producer, OpenReplicator p) throws Exception;

	public void work(RowMap row, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {
	 	if ( isStartBootstrapRow(row) ) {
			startBootstrap(row, schema, producer, replicator);
		} else if ( isCompleteBootstrapRow(row) ) {
			completeBootstrap(row, schema, producer, replicator);
		}
	}
}
