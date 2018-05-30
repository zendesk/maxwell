package com.zendesk.maxwell.core.producer.impl.redis;

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
public class RedisExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "redis";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
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
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellRedisProducer(context, context.getConfig().getRedisPubChannel(), context.getConfig().getRedisListKey(), context.getConfig().getRedisType());
	}
}
