package com.zendesk.maxwell.core.producer.impl.kafka;

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
public class KafkaExtensionConfigurator implements ExtensionConfigurator<Producer> {
	@Override
	public String getExtensionIdentifier() {
		return "profiler";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "kafka_version", "kafka client library version: 0.8.2.2|0.9.0.1|0.10.0.1|0.10.2.1|0.11.0.1");
		context.addOptionWithRequiredArgument( "kafka_partition_by", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka_partition_columns", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka_partition_by_fallback", "[deprecated]");
		context.addOptionWithRequiredArgument( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" );
		context.addOptionWithRequiredArgument( "kafka_partition_hash", "default|murmur3, hash function for partitioning" );
		context.addOptionWithRequiredArgument( "kafka_topic", "optionally provide a topic name to push to. default: maxwell" );
		context.addOptionWithRequiredArgument( "kafka_key_format", "how to format the kafka key; array|hash" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new MaxwellKafkaProducer(context, context.getConfig().getKafkaProperties(), context.getConfig().getKafkaTopic());
	}
}
