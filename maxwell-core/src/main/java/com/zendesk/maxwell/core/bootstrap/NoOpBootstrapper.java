package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.row.RowMapFactory;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.core.replication.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger( NoOpBootstrapper.class );

	public NoOpBootstrapper(MaxwellSystemContext context, RowMapFactory rowMapFactory) { super( context, rowMapFactory); }

	@Override
	public boolean shouldSkip(RowMap row) {
		return false;
	}

	@Override
	public void startBootstrap(RowMap startBootstrapRow, Producer producer, Replicator replicator) throws Exception {}

	@Override
	public void completeBootstrap(RowMap completeBootstrapRow, Producer producer, Replicator replicator) throws Exception {}

	@Override
	public void resume(Producer producer, Replicator replicator) throws Exception {}

	@Override
	public boolean isRunning( ) {
		return false;
	}

	@Override
	public void work(RowMap row, Producer producer, Replicator replicator) throws Exception {}

}
