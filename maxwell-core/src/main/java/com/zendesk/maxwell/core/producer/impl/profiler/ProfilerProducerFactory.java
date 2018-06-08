package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class ProfilerProducerFactory implements ProducerFactory {
	@Override
	public Producer createProducer(MaxwellContext context) {
		return new ProfilerProducer(context);
	}
}
