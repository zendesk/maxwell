package com.zendesk.maxwell.benchmark;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

public class BenchmarkProducerFactory implements ProducerFactory {
	private long skipRows;
	public BenchmarkProducerFactory(long skipRows) {
		this.skipRows = skipRows;
	}
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		return new BenchmarkProducer(context, skipRows);
	}
}
