package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class BufferedProducerConfigurator implements ProducerConfigurator<BufferedProducerConfiguration> {
	@Override
	public String getExtensionIdentifier() {
		return "buffer";
	}

	@Override
	public Optional<BufferedProducerConfiguration> parseConfiguration(Properties configurationValues) {
		return Optional.of(new BufferedProducerConfiguration());
	}

	@Override
	public Producer createInstance(MaxwellContext context, BufferedProducerConfiguration configuration) {
		return new BufferedProducer(context, configuration.getBufferedProducerSize());
	}
}
