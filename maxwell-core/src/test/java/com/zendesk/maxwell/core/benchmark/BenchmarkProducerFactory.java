package com.zendesk.maxwell.core.benchmark;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.producer.AbstractProducer;

public class BenchmarkProducerFactory implements ProducerFactory {
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		return new BenchmarkProducer(context);
	}
}
