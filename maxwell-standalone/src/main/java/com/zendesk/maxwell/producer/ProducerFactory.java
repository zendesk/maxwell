package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;

public interface ProducerFactory {
	AbstractProducer createProducer(MaxwellContext context);
}
