package com.zendesk.maxwell.standalone.benchmark;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.producer.AbstractProducer;

public class BenchmarkProducerFactory implements ProducerFactory {
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		return new BenchmarkProducer(context);
	}
}
