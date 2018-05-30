package com.zendesk.maxwell.core.producer.impl.kinesis;

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
public class KinesisExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "kinesis";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithOptionalArgument( "kinesis_stream", "kinesis stream name" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellKinesisProducer(context, context.getConfig().getKinesisStream());
	}
}
