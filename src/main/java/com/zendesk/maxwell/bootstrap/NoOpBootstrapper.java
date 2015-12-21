package com.zendesk.maxwell.bootstrap;

import com.google.code.or.OpenReplicator;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellReplicator;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger( MaxwellReplicator.class );

	public NoOpBootstrapper(MaxwellContext context) { super( context ); }

	@Override
	public boolean isStartBootstrapRow(RowMap row) {
		return false;
	}

	@Override
	public boolean isCompleteBootstrapRow(RowMap row) {
		return false;
	}

	@Override
	public boolean isBootstrapRow(RowMap row) {
		return false;
	}

	@Override
	public boolean shouldSkip(RowMap row) {
		return false;
	}

	@Override
	public void startBootstrap(RowMap startBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, Schema schema, AbstractProducer producer, OpenReplicator replicator) throws Exception {}

	@Override
	public void resume(Schema schema, AbstractProducer producer, OpenReplicator p) throws Exception {}

}
