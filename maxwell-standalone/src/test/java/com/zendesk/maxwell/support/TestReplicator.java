package com.zendesk.maxwell.support;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.replication.AbstractReplicator;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.util.RunState;

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
