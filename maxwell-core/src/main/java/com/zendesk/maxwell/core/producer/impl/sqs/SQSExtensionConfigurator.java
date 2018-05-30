package com.zendesk.maxwell.core.producer.impl.sqs;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfiguration;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.core.config.ExtensionType;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class SQSExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "sqs";
	}

	@Override
	public ExtensionType getExtensionType() {
		return null;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "sqs_queue_uri", "SQS Queue uri" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellSQSProducer(context, context.getConfig().getSqsQueueUri());
	}
}
