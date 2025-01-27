package com.zendesk.maxwell.producer;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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
		MaxwellConfig config = context.getConfig();

		try {
			if ( config.rabbitmqURI != null ) {
				factory.setUri(config.rabbitmqURI);
			}

			if ( config.rabbitmqHost != null ) {
				factory.setHost(config.rabbitmqHost);
			} else if ( config.rabbitmqURI == null ) {
				factory.setHost("localhost");
			}

			if ( config.rabbitmqPort != null ) {
				factory.setPort(config.rabbitmqPort);
			}

			if ( config.rabbitmqUser != null ) {
				factory.setUsername(config.rabbitmqUser);
			}

			if ( config.rabbitmqPass != null ) {
				factory.setPassword(config.rabbitmqPass);
			}

			if ( config.rabbitmqVirtualHost != null ) {
				factory.setVirtualHost(config.rabbitmqVirtualHost);
			}

			if ( config.rabbitmqHandshakeTimeout != null ) {
				factory.setHandshakeTimeout(config.rabbitmqHandshakeTimeout);
			}

			if ( config.rabbitmqUseSSL ) {
				factory.useSslProtocol(SSLContext.getDefault());
			}

			this.channel = factory.newConnection().createChannel();
			if(context.getConfig().rabbitmqDeclareExchange) {
				this.channel.exchangeDeclare(exchangeName, context.getConfig().rabbitmqExchangeType, context.getConfig().rabbitMqExchangeDurable, context.getConfig().rabbitMqExchangeAutoDelete, null);
			}
		} catch (IOException | TimeoutException e) {
			throw new RuntimeException(e);
		} catch (NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
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
			LOGGER.debug("->  routing key:{}, partition:{}", routingKey, value);
		}
	}

	private String getRoutingKeyFromTemplate(RowMap r) {
		String table = r.getTable();

		if ( table == null )
			table = "";

		return context
				.getConfig()
				.rabbitmqRoutingKeyTemplate
				.replace("%db%", r.getDatabase())
				.replace("%table%", table);
	}
}
