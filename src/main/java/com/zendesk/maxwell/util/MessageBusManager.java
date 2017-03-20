package com.zendesk.maxwell.util;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.vertx.core.json.JsonObject;

public class MessageBusManager {
    static final Logger LOGGER = LoggerFactory.getLogger(MessageBusManager.class);
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
	private static ConnectionFactory _factory;
	private static Connection _connection;
	private static ConcurrentMap<Long, Channel> _channels = new ConcurrentHashMap<>();
	private static MessageBusManager _instance;

	private MessageBusManager() {
		
	}
	
	public static MessageBusManager getInstance() {
		if (_instance == null) {
			_instance = new MessageBusManager();
			return _instance;
		}
		return _instance;
	}
	
	// connect to message bus
	public void start(String rabbitmq_host, int rabbitmq_port, String rabbitmq_user, String rabbitmq_passsword) {
	
		_factory = new ConnectionFactory();
		_factory.setHost(rabbitmq_host);
		_factory.setUsername(rabbitmq_user);
		_factory.setPassword(rabbitmq_passsword);
		_factory.setPort(rabbitmq_port);

        try {
			_connection = _factory.newConnection();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} 
	}
	
	// disconnect and free all the resources
	public void stop() throws IOException, TimeoutException {
		for (Channel channel : _channels.values()) {
			channel.close();
		}
		_connection.close();
	}
	
	
	public static Connection getConnection() {
		return _connection;
	}

	
	public static Channel createNewChannel() throws IOException{
		Channel channel = null;
		channel= _connection.createChannel();
		return channel;
		
	}
	
	public static void closeChannel(Channel channel,String exchangeName) throws IOException, TimeoutException{
		LOGGER.info("closing the channel associate with exchange "+exchangeName);
		channel.close();
		
	}
	
	public static void deleteExchange(Channel channel,String exchangeName) throws IOException{
		LOGGER.info("removing exchange " + exchangeName);
		channel.exchangeDelete(exchangeName);
	}
	
	public static void exchangeDeclare(Channel channel,String exchangeName,String type) throws IOException{
		channel.exchangeDeclare(exchangeName, type);
	}
	
	public static void sendDirectNotification(String routingKey, JsonObject jsonBody, long id) throws IOException{
        JsonObject properties = getMessageProperties();
    	Channel channel = _channels.putIfAbsent(id, _connection.createChannel());
    	if (channel == null) {
    		channel = _channels.get(id);
    	}
		channel.basicPublish ("",routingKey, fromJson(properties), jsonBody.encodePrettily().getBytes());
	}
	
	public void sendNotificationToTopicExchangeForSpecifiedDbName(String dbName, String routingKey, String notification, long threadId, String exchangeType) {
    	
    	try {
    		JsonObject properties = getMessageProperties();
    		
    		Channel channel = _channels.putIfAbsent(threadId, _connection.createChannel());
    		if (channel == null) {
    			channel = _channels.get(threadId);
    		}

	        channel.exchangeDeclare(dbName, exchangeType, false, true, null);
			channel.basicPublish(dbName ,routingKey, fromJson(properties), notification.getBytes());

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	// Format messages before publishing to rabbitmq
    private static AMQP.BasicProperties fromJson(JsonObject json) {
        if (json == null) return new AMQP.BasicProperties();

        return new AMQP.BasicProperties.Builder()
          .contentType(json.getString("contentType"))
          .contentEncoding(json.getString("contentEncoding"))
          .headers(asMap(json.getJsonObject("headers")))
          .deliveryMode(json.getInteger("deliveryMode"))
          .priority(json.getInteger("priority"))
          .correlationId(json.getString("correlationId"))
          .replyTo(json.getString("replyTo"))
          .expiration(json.getString("expiration"))
          .messageId(json.getString("messageId"))
          .timestamp(parseDate(json.getString("timestamp")))
          .type(json.getString("type"))
          .userId(json.getString("userId"))
          .appId(json.getString("appId"))
          .clusterId(json.getString("clusterId")).build();
      }
    
    private static Map<String, Object> asMap(JsonObject json) {
        if (json == null) return null;

        return json.getMap();
      }
    
    private static Date parseDate(String date) {
        if (date == null) return null;

        OffsetDateTime odt = OffsetDateTime.parse(date, dateTimeFormatter);
        return Date.from(odt.toInstant());
      }
    
    private static JsonObject getMessageProperties(){
        Map<String, Object> props = new HashMap<>();
        props.put("contentType", "application/json");
        JsonObject properties = new JsonObject(props);
        return properties;
    }
	
}