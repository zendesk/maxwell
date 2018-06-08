package com.zendesk.maxwell.producer.sqs;

import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.ProducerConfigurator;
import com.zendesk.maxwell.api.producer.ProducerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class SQSProducerConfigurator implements ProducerConfigurator {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public SQSProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "sqs";
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "sqs_queue_uri", "SQS Queue uri" );
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		final String sqsQueueName = configurationSupport.fetchOption("sqs_queue_uri", configurationValues, null);
		return Optional.of(new SQSProducerConfiguration(sqsQueueName));
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return SQSProducerFactory.class;
	}
}
