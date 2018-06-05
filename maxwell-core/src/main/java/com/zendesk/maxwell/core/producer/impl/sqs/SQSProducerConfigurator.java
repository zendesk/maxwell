package com.zendesk.maxwell.core.producer.impl.sqs;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class SQSProducerConfigurator implements ProducerConfigurator<SQSProducerConfiguration> {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public SQSProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "sqs";
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "sqs_queue_uri", "SQS Queue uri" );
	}

	@Override
	public Optional<SQSProducerConfiguration> parseConfiguration(Properties configurationValues) {
		final String sqsQueueName = configurationSupport.fetchOption("sqs_queue_uri", configurationValues, null);
		return Optional.of(new SQSProducerConfiguration(sqsQueueName));
	}

	@Override
	public Producer createInstance(MaxwellContext context, SQSProducerConfiguration configuration) {
		return new MaxwellSQSProducer(context, configuration.getSqsQueueUri());
	}
}
