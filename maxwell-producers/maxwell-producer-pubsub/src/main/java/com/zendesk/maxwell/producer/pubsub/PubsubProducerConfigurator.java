package com.zendesk.maxwell.producer.pubsub;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.core.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Service
public class PubsubProducerConfigurator implements ProducerConfigurator {

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
		String projectId = configurationSupport.fetchOption("pubsub_project_id", configurationValues, null);
		String topic = configurationSupport.fetchOption("pubsub_topic", configurationValues, "maxwell");
		String ddlTopic = configurationSupport.fetchOption("ddl_pubsub_topic", configurationValues, topic);
		return Optional.of(new PubsubProducerConfiguration(projectId, topic, ddlTopic));
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		try {
			PubsubProducerConfiguration pubsubProducerConfiguration = (PubsubProducerConfiguration)configuration;
			return new MaxwellPubsubProducer(maxwellContext, pubsubProducerConfiguration);
		} catch (IOException e) {
			throw new ProducerInstantiationException(e);
		}
	}
}
