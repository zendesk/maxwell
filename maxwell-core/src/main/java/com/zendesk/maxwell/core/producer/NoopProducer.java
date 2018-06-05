package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.row.RowMap;

public class NoopProducer extends AbstractProducer {

	public NoopProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {

	}
}
