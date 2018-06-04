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
	private final RabbitmqProducerConfiguration config;

	public RabbitmqProducer(MaxwellContext context) {
		super(context);
		config = context.getConfig().getProducerConfigOrThrowExceptionWhenNotDefined();

		exchangeName = config.getRabbitmqExchange();
		props = config.isRabbitmqMessagePersistent() ? MessageProperties.MINIMAL_PERSISTENT_BASIC : null;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(config.getRabbitmqHost());
		factory.setPort(config.getRabbitmqPort());
		factory.setUsername(config.getRabbitmqUser());
		factory.setPassword(config.getRabbitmqPass());
		factory.setVirtualHost(config.getRabbitmqVirtualHost());
		try {
			this.channel = factory.newConnection().createChannel();
			if(config.isRabbitmqDeclareExchange()) {
				this.channel.exchangeDeclare(exchangeName, config.getRabbitmqExchangeType(), config.isRabbitMqExchangeDurable(), config.isRabbitMqExchangeAutoDelete(), null);
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
		return config.getRabbitmqRoutingKeyTemplate()
				.replace("%db%", r.getDatabase())
				.replace("%table%", r.getTable());
	}
}
