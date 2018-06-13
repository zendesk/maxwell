package com.zendesk.maxwell.producer.rabbitmq;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.config.CommandLineOptionParserContext;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerConfiguration;
import com.zendesk.maxwell.core.producer.ProducerConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class RabbitmqProducerConfigurator implements ProducerConfigurator {

	private final ConfigurationSupport configurationSupport;

	@Autowired
	public RabbitmqProducerConfigurator(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	@Override
	public String getIdentifier() {
		return "rabbitmq";
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
	public Optional<ProducerConfiguration> parseConfiguration(Properties configurationValues) {
		RabbitmqProducerConfiguration config = new RabbitmqProducerConfiguration();
		config.setRabbitmqHost(configurationSupport.fetchOption("rabbitmq_host", configurationValues, "localhost"));
		config.setRabbitmqPort(Integer.parseInt(configurationSupport.fetchOption("rabbitmq_port", configurationValues, "5672")));
		config.setRabbitmqUser(configurationSupport.fetchOption("rabbitmq_user", configurationValues, "guest"));
		config.setRabbitmqPass(configurationSupport.fetchOption("rabbitmq_pass", configurationValues, "guest"));
		config.setRabbitmqVirtualHost(configurationSupport.fetchOption("rabbitmq_virtual_host", configurationValues, "/"));
		config.setRabbitmqExchange(configurationSupport.fetchOption("rabbitmq_exchange", configurationValues, "maxwell"));
		config.setRabbitmqExchangeType(configurationSupport.fetchOption("rabbitmq_exchange_type", configurationValues, "fanout"));
		config.setRabbitMqExchangeDurable(configurationSupport.fetchBooleanOption("rabbitmq_exchange_durable", configurationValues, false));
		config.setRabbitMqExchangeAutoDelete(configurationSupport.fetchBooleanOption("rabbitmq_exchange_autodelete", configurationValues, false));
		config.setRabbitmqRoutingKeyTemplate(configurationSupport.fetchOption("rabbitmq_routing_key_template", configurationValues, "%db%.%table%"));
		config.setRabbitmqMessagePersistent(configurationSupport.fetchBooleanOption("rabbitmq_message_persistent", configurationValues, false));
		config.setRabbitmqDeclareExchange(configurationSupport.fetchBooleanOption("rabbitmq_declare_exchange", configurationValues, true));
		return Optional.of(config);
	}

	@Override
	public Producer configure(MaxwellContext maxwellContext, ProducerConfiguration configuration) {
		RabbitmqProducerConfiguration rabbitmqProducerConfiguration = (RabbitmqProducerConfiguration)configuration;
		return new RabbitmqProducer(maxwellContext, rabbitmqProducerConfiguration);
	}
}
