package com.zendesk.maxwell.core.producer.impl.pubsub;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Service
public class PubsubExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "pubsub";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" );
		context.addOptionWithRequiredArgument( "pubsub_topic", "optionally provide a pubsub topic to push to. default: maxwell" );
		context.addOptionWithRequiredArgument( "ddl_pubsub_topic", "optionally provide an alternate pubsub topic to push DDL records to. default: pubsub_topic" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		try {
			return new MaxwellPubsubProducer(context, context.getConfig().getPubsubProjectId(), context.getConfig().getPubsubTopic(), context.getConfig().getDdlPubsubTopic());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
