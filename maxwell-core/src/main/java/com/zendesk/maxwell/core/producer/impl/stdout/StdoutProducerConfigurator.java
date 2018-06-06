package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import org.springframework.stereotype.Service;

@Service
public class StdoutProducerConfigurator implements ProducerConfigurator<ProducerConfiguration> {
	@Override
	public String getExtensionIdentifier() {
		return "stdout";
	}

	@Override
	public Producer createInstance(MaxwellContext context, ProducerConfiguration configuration) {
		return new StdoutProducer(context);
	}
}
