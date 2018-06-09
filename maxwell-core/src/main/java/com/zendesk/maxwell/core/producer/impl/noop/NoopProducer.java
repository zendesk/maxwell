package com.zendesk.maxwell.core.producer.impl.noop;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.AbstractProducer;
import com.zendesk.maxwell.api.row.RowMap;

public class NoopProducer extends AbstractProducer {

	public NoopProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {

	}
}