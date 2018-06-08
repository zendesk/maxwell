package com.zendesk.maxwell.core.producer.impl.noop;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class NoopProducerFactory implements ProducerFactory {
	@Override
	public Producer createProducer(MaxwellContext context) {
		return new NoopProducer(context);
	}
}
