package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import com.zendesk.maxwell.core.producer.Producer;
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
