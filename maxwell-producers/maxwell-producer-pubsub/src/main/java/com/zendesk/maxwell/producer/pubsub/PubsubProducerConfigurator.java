package com.zendesk.maxwell.producer.pubsub;

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
public class PubsubProducerConfigurator implements ProducerConfigurator{

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public PubsubProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "pubsub";
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" );
		context.addOptionWithRequiredArgument( "pubsub_topic", "optionally provide a pubsub topic to push to. default: maxwell" );
		context.addOptionWithRequiredArgument( "ddl_pubsub_topic", "optionally provide an alternate pubsub topic to push DDL records to. default: pubsub_topic" );
	}

	@Override
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		PubsubProducerConfiguration config = new PubsubProducerConfiguration();
		config.setPubsubProjectId(configurationSupport.fetchOption("pubsub_project_id", configurationValues, null));
		config.setPubsubTopic(configurationSupport.fetchOption("pubsub_topic", configurationValues, "maxwell"));
		config.setDdlPubsubTopic(configurationSupport.fetchOption("ddl_pubsub_topic", configurationValues, config.getPubsubTopic()));
		return Optional.of(config);
	}

	@Override
	public Class<? extends ProducerFactory> getFactory() {
		return PubSubProducerFactory.class;
	}
}
