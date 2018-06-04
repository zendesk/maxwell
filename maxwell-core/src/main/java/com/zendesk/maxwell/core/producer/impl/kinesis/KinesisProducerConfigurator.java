package com.zendesk.maxwell.core.producer.impl.kinesis;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class KinesisProducerConfigurator implements ExtensionConfigurator<Producer> {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public KinesisProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "kinesis";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithOptionalArgument( "kinesis_stream", "kinesis stream name" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties configurationValues) {
		String stream = configurationSupport.fetchOption("kinesis_stream", configurationValues, null);
		boolean md5keys = configurationSupport.fetchBooleanOption("kinesis_md5_keys", configurationValues, false);
		return Optional.of(new KinesisProducerConfiguration(stream, md5keys));
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellKinesisProducer(context);
	}
}
