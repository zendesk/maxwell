package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import joptsimple.OptionSet;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class BufferedProducerConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "buffer";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(OptionSet commandLineArguments, Properties configurationValues) {
		return Optional.of(new BufferedProducerConfiguration());
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new BufferedProducer(context, context.getConfig().<BufferedProducerConfiguration>getProducerConfigOrThrowExceptionWhenNotDefined().getBufferedProducerSize());
	}
}
