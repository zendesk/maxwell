package com.zendesk.maxwell.core.producer.impl.redis;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import joptsimple.OptionSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class RedisProducerConfigurator implements ExtensionConfigurator<Producer> {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public RedisProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "redis";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "redis_host", "Host of Redis server" );
		context.addOptionWithRequiredArgument( "redis_port", "Port of Redis server" );
		context.addOptionWithRequiredArgument( "redis_auth", "Authentication key for a password-protected Redis server" );
		context.addOptionWithRequiredArgument( "redis_database", "Database of Redis server" );
		context.addOptionWithRequiredArgument( "redis_pub_channel", "Redis Pub/Sub channel for publishing records" );
		context.addOptionWithRequiredArgument( "redis_list_key", "Redis LPUSH List Key for adding to a queue" );
		context.addOptionWithRequiredArgument( "redis_type", "[pubsub|lpush] Selects either Redis Pub/Sub or LPUSH. Defaults to 'pubsub'" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(OptionSet commandLineArguments, Properties configurationValues) {
		RedisProducerConfiguration config = new RedisProducerConfiguration();
		config.setRedisHost(configurationSupport.fetchOption("redis_host", commandLineArguments, configurationValues, "localhost"));
		config.setRedisPort(Integer.parseInt(configurationSupport.fetchOption("redis_port", commandLineArguments, configurationValues, "6379")));
		config.setRedisAuth(configurationSupport.fetchOption("redis_auth", commandLineArguments, configurationValues, null));
		config.setRedisDatabase(Integer.parseInt(configurationSupport.fetchOption("redis_database", commandLineArguments, configurationValues, "0")));
		config.setRedisPubChannel(configurationSupport.fetchOption("redis_pub_channel", commandLineArguments, configurationValues, "maxwell"));
		config.setRedisListKey(configurationSupport.fetchOption("redis_list_key", commandLineArguments, configurationValues, "maxwell"));
		config.setRedisType(configurationSupport.fetchOption("redis_type", commandLineArguments, configurationValues, "pubsub"));
		return Optional.of(config);
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellRedisProducer(context);
	}
}
