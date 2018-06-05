package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import com.zendesk.maxwell.core.producer.Producer;
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
