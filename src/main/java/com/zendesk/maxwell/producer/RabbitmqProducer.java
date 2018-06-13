package com.zendesk.maxwell.producer;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitmqProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqProducer.class);
	private static String exchangeName;
	private static BasicProperties props;
	private Channel channel;
	public RabbitmqProducer(MaxwellContext context) {
		super(context);
		exchangeName = context.getConfig().rabbitmqExchange;
		props = context.getConfig().rabbitmqMessagePersistent ? MessageProperties.MINIMAL_PERSISTENT_BASIC : null;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(context.getConfig().rabbitmqHost);
		factory.setPort(context.getConfig().rabbitmqPort);
		factory.setUsername(context.getConfig().rabbitmqUser);
		factory.setPassword(context.getConfig().rabbitmqPass);
		factory.setVirtualHost(context.getConfig().rabbitmqVirtualHost);
		try {
			this.channel = factory.newConnection().createChannel();
			if(context.getConfig().rabbitmqDeclareExchange) {
				this.channel.exchangeDeclare(exchangeName, context.getConfig().rabbitmqExchangeType, context.getConfig().rabbitMqExchangeDurable, context.getConfig().rabbitMqExchangeAutoDelete, null);
			}
		} catch (IOException | TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());

			return;
		}

		String value = r.toJSON(outputConfig);
		String routingKey = getRoutingKeyFromTemplate(r);

		channel.basicPublish(exchangeName, routingKey, props, value.getBytes());
		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  routing key:" + routingKey + ", partition:" + value);
		}
	}

	private String getRoutingKeyFromTemplate(RowMap r) {
		return context
				.getConfig()
				.rabbitmqRoutingKeyTemplate
				.replace("%db%", r.getDatabase())
				.replace("%table%", r.getTable());
	}
}
