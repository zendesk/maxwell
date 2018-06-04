package com.zendesk.maxwell.core.producer.impl.rabbitmq;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import joptsimple.OptionSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class RabbitmqProducerConfigurator implements ExtensionConfigurator<Producer> {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public RabbitmqProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getExtensionIdentifier() {
		return "rabbitmq";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PRODUCER;
	}

	@Override
	public void configureCommandLineOptions(CommandLineOptionParserContext context) {
		context.addOptionWithRequiredArgument( "rabbitmq_user", "Username of Rabbitmq connection. Default is guest" );
		context.addOptionWithRequiredArgument( "rabbitmq_pass", "Password of Rabbitmq connection. Default is guest" );
		context.addOptionWithRequiredArgument( "rabbitmq_host", "Host of Rabbitmq machine" );
		context.addOptionWithRequiredArgument( "rabbitmq_port", "Port of Rabbitmq machine" );
		context.addOptionWithRequiredArgument( "rabbitmq_virtual_host", "Virtual Host of Rabbitmq" );
		context.addOptionWithRequiredArgument( "rabbitmq_exchange", "Name of exchange for rabbitmq publisher" );
		context.addOptionWithRequiredArgument( "rabbitmq_exchange_type", "Exchange type for rabbitmq" );
		context.addOptionWithOptionalArgument( "rabbitmq_exchange_durable", "Exchange durability. Default is disabled" );
		context.addOptionWithOptionalArgument( "rabbitmq_exchange_autodelete", "If set, the exchange is deleted when all queues have finished using it. Defaults to false" );
		context.addOptionWithRequiredArgument( "rabbitmq_routing_key_template", "A string template for the routing key, '%db%' and '%table%' will be substituted. Default is '%db%.%table%'." );
		context.addOptionWithOptionalArgument( "rabbitmq_message_persistent", "Message persistence. Defaults to false" );
		context.addOptionWithOptionalArgument( "rabbitmq_declare_exchange", "Should declare the exchange for rabbitmq publisher. Defaults to true" );
	}

	@Override
	public Optional<ExtensionConfiguration> parseConfiguration(OptionSet commandLineArguments, Properties configurationValues) {
		RabbitmqProducerConfiguration config = new RabbitmqProducerConfiguration();
		config.setRabbitmqHost(configurationSupport.fetchOption("rabbitmq_host", commandLineArguments, configurationValues, "localhost"));
		config.setRabbitmqPort(Integer.parseInt(configurationSupport.fetchOption("rabbitmq_port", commandLineArguments, configurationValues, "5672")));
		config.setRabbitmqUser(configurationSupport.fetchOption("rabbitmq_user", commandLineArguments, configurationValues, "guest"));
		config.setRabbitmqPass(configurationSupport.fetchOption("rabbitmq_pass", commandLineArguments, configurationValues, "guest"));
		config.setRabbitmqVirtualHost(configurationSupport.fetchOption("rabbitmq_virtual_host", commandLineArguments, configurationValues, "/"));
		config.setRabbitmqExchange(configurationSupport.fetchOption("rabbitmq_exchange", commandLineArguments, configurationValues, "maxwell"));
		config.setRabbitmqExchangeType(configurationSupport.fetchOption("rabbitmq_exchange_type", commandLineArguments, configurationValues, "fanout"));
		config.setRabbitMqExchangeDurable(configurationSupport.fetchBooleanOption("rabbitmq_exchange_durable", commandLineArguments, configurationValues, false));
		config.setRabbitMqExchangeAutoDelete(configurationSupport.fetchBooleanOption("rabbitmq_exchange_autodelete", commandLineArguments, configurationValues, false));
		config.setRabbitmqRoutingKeyTemplate(configurationSupport.fetchOption("rabbitmq_routing_key_template", commandLineArguments, configurationValues, "%db%.%table%"));
		config.setRabbitmqMessagePersistent(configurationSupport.fetchBooleanOption("rabbitmq_message_persistent", commandLineArguments, configurationValues, false));
		config.setRabbitmqDeclareExchange(configurationSupport.fetchBooleanOption("rabbitmq_declare_exchange", commandLineArguments, configurationValues, true));
		return Optional.of(config);
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new RabbitmqProducer(context);
	}

}
