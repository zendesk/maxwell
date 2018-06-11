package com.zendesk.maxwell.producer.kinesis;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class KinesisProducerConfigurator implements ProducerConfigurator {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public KinesisProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "kinesis";
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithOptionalArgument( "kinesis_stream", "kinesis stream name" );
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		String stream = configurationSupport.fetchOption("kinesis_stream", configurationValues, null);
		boolean md5keys = configurationSupport.fetchBooleanOption("kinesis_md5_keys", configurationValues, false);
		return Optional.of(new KinesisProducerConfiguration(stream, md5keys));
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		KinesisProducerConfiguration kinesisProducerConfiguration = (KinesisProducerConfiguration)configuration;
		return new MaxwellKinesisProducer(maxwellContext, kinesisProducerConfiguration);
	}
}
