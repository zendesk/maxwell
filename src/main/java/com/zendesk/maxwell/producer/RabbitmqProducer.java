package com.zendesk.maxwell.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitmqProducer extends AbstractProducer {

	static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqProducer.class);
	private static String exchangeName;
	private Connection connection;
	private Channel channel;
	public RabbitmqProducer(MaxwellContext context) {
		super(context);
		exchangeName = context.getConfig().rabbitmqExchange;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(context.getConfig().rabbitmqHost);
		try {
			this.connection = factory.newConnection();
			this.channel = connection.createChannel();
			this.channel.exchangeDeclare(exchangeName, context.getConfig().rabbitmqExchangeType);
		} catch (IOException | TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		String value = r.toJSON(outputConfig);
		if (value == null) {
			return;
		}
		String routingKey = r.getDatabase() + "." + r.getTable();

		channel.basicPublish(exchangeName, routingKey, null, value.getBytes());
		if ( r.isTXCommit() ) {
			context.setPosition(r.getPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  routing key:" + routingKey + ", partition:" + value);
		}
	}
}
