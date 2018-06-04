package com.zendesk.maxwell.core.producer.impl.pubsub;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Service
public class PubsubProducerConfigurator implements ExtensionConfigurator<Producer> {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public PubsubProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "pubsub";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" );
		context.addOptionWithRequiredArgument( "pubsub_topic", "optionally provide a pubsub topic to push to. default: maxwell" );
		context.addOptionWithRequiredArgument( "ddl_pubsub_topic", "optionally provide an alternate pubsub topic to push DDL records to. default: pubsub_topic" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties configurationValues) {
		PubsubProducerConfiguration config = new PubsubProducerConfiguration();
		config.setPubsubProjectId(configurationSupport.fetchOption("pubsub_project_id", configurationValues, null));
		config.setPubsubTopic(configurationSupport.fetchOption("pubsub_topic", configurationValues, "maxwell"));
		config.setDdlPubsubTopic(configurationSupport.fetchOption("ddl_pubsub_topic", configurationValues, config.getPubsubTopic()));
		return Optional.of(config);
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		try {
			PubsubProducerConfiguration config = context.getConfig().getProducerConfigOrThrowExceptionWhenNotDefined();
			return new MaxwellPubsubProducer(context, config.getPubsubProjectId(), config.getPubsubTopic(), config.getDdlPubsubTopic());
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
