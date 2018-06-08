package com.zendesk.maxwell.core.producer.impl.file;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import com.zendesk.maxwell.api.producer.ProducerInstantiationException;

import java.io.IOException;

public class FileProducerFactory implements ProducerFactory {
	@Override
	public Producer createProducer(MaxwellContext context) {
		try {
			FileProducerConfiguration configuration = (FileProducerConfiguration)context.getConfig().getProducerConfiguration();
			return new FileProducer(context, configuration.getOutputFile());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
