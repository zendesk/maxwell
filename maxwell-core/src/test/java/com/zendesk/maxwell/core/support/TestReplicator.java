package com.zendesk.maxwell.core.support;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.monitoring.NoOpMetrics;
import com.zendesk.maxwell.core.producer.impl.buffered.BufferedProducer;
import com.zendesk.maxwell.core.replication.AbstractReplicator;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.schema.Schema;
import com.zendesk.maxwell.core.schema.SchemaStoreException;
import com.zendesk.maxwell.core.util.RunState;

public class TestReplicator extends AbstractReplicator {

	public TestReplicator(MaxwellContext context) {
		super(
			null, null, null,
			new BufferedProducer(context, 10), new NoOpMetrics(), null, null
		);
	}

	public BufferedProducer getProducer() {
		return (BufferedProducer) producer;
	}

	public void processRow(RowMap row) throws Exception {
		super.processRow(row);
	}

	public RunState getState() {
		return taskState.getState();
	}

	@Override
	public void startReplicator() throws Exception {
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		return null;
	}

	@Override
	public RowMap getRow() throws Exception {
		return null;
	}
}
