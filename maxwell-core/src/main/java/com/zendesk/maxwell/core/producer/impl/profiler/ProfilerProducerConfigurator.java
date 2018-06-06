package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import org.springframework.stereotype.Service;

@Service
public class ProfilerProducerConfigurator implements ProducerConfigurator<ProducerConfiguration> {

	@Override
	public String getExtensionIdentifier() {
		return "profiler";
	}

	@Override
	public Producer createInstance(MaxwellContext context, ProducerConfiguration configuration) {
		return  new ProfilerProducer(context);
	}

}
