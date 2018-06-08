package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProfilerProducerConfigurator implements ProducerConfigurator {

	@Override
	public String getIdentifier() {
		return "profiler";
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return ProfilerProducerFactory.class;
	}
}
