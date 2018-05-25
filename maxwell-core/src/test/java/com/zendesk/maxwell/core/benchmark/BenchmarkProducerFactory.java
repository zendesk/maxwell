package com.zendesk.maxwell.core.benchmark;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.producer.ProducerFactory;

public class BenchmarkProducerFactory implements ProducerFactory {
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		return new BenchmarkProducer(context);
	}
}
