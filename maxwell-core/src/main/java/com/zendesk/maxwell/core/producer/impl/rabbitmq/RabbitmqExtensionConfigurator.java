package com.zendesk.maxwell.core.producer.impl.rabbitmq;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.producer.Producer;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Properties;

@Service
public class RabbitmqExtensionConfigurator implements ExtensionConfigurator<Producer> {

	@Override
	public String getExtensionIdentifier() {
		return "rabbitmq";
	}

	@Override
	public ExtensionType getExtensionType() {
		return ExtensionType.PROVIDER;
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
	public Optional<ExtensionConfiguration> parseConfiguration(Properties commandLineArguments, Properties configurationValues) {
		return Optional.empty();
	}

	@Override
	public Producer createInstance(MaxwellContext context) {
		return new RabbitmqProducer(context);
	}

}
