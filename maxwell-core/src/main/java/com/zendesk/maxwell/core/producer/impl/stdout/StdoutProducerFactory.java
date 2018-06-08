package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class StdoutProducerFactory implements ProducerFactory {
	@Override
	public Producer createProducer(MaxwellContext context) {
		return new StdoutProducer(context);
	}
}
