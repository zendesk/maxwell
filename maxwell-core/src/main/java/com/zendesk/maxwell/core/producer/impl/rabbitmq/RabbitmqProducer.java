package com.zendesk.maxwell.core.producer.impl.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.row.RowMap;
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
		exchangeName = context.getConfig().getRabbitmqExchange();
		props = context.getConfig().isRabbitmqMessagePersistent() ? MessageProperties.MINIMAL_PERSISTENT_BASIC : null;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(context.getConfig().getRabbitmqHost());
		factory.setPort(context.getConfig().getRabbitmqPort());
		factory.setUsername(context.getConfig().getRabbitmqUser());
		factory.setPassword(context.getConfig().getRabbitmqPass());
		factory.setVirtualHost(context.getConfig().getRabbitmqVirtualHost());
		try {
			this.channel = factory.newConnection().createChannel();
			if(context.getConfig().isRabbitmqDeclareExchange()) {
				this.channel.exchangeDeclare(exchangeName, context.getConfig().getRabbitmqExchangeType(), context.getConfig().isRabbitMqExchangeDurable(), context.getConfig().isRabbitMqExchangeAutoDelete(), null);
			}
		} catch (IOException | TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getPosition());

			return;
		}

		String value = r.toJSON(outputConfig);
		String routingKey = getRoutingKeyFromTemplate(r);

		channel.basicPublish(exchangeName, routingKey, props, value.getBytes());
		if ( r.isTXCommit() ) {
			context.setPosition(r.getPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  routing key:" + routingKey + ", partition:" + value);
		}
	}

	private String getRoutingKeyFromTemplate(RowMap r) {
		return context
				.getConfig()
				.getRabbitmqRoutingKeyTemplate()
				.replace("%db%", r.getDatabase())
				.replace("%table%", r.getTable());
	}
}
