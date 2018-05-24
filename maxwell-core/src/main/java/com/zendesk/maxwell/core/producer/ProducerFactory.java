package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.MaxwellContext;

public interface ProducerFactory {
	AbstractProducer createProducer(MaxwellContext context);
}
