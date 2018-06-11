package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class BufferedProducerConfigurator implements ProducerConfigurator {
	@Override
	public String getIdentifier() {
		return "buffer";
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		return Optional.of(new BufferedProducerConfiguration());
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		BufferedProducerConfiguration bufferedProducerConfiguration = (BufferedProducerConfiguration)configuration;
		return new BufferedProducer(maxwellContext, bufferedProducerConfiguration.getBufferedProducerSize());
	}

}
