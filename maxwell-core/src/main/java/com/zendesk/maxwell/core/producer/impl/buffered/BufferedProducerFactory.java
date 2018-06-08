package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;

public class BufferedProducerFactory implements ProducerFactory {
	@Override
	public Producer createProducer(MaxwellContext context) {
		BufferedProducerConfiguration configuration = (BufferedProducerConfiguration)context.getConfig().getProducerConfiguration();
		return new BufferedProducer(context, configuration.getBufferedProducerSize());
	}
}
