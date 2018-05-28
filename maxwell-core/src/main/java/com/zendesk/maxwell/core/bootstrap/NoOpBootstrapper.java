package com.zendesk.maxwell.core.bootstrap;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpBootstrapper extends AbstractBootstrapper {

	static final Logger LOGGER = LoggerFactory.getLogger( NoOpBootstrapper.class );

	public NoOpBootstrapper(MaxwellContext context) { super( context ); }

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
