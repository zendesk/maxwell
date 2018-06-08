package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import org.springframework.stereotype.Service;

@Service
public class StdoutProducerConfigurator implements ProducerConfigurator {
	@Override
	public String getIdentifier() {
		return "stdout";
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return StdoutProducerFactory.class;
	}
}
