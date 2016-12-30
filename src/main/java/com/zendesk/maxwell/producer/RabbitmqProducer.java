package com.zendesk.maxwell.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by hitesh on 30/12/16.
 */
public class RabbitmqProducer extends AbstractProducer {
    private static String exchangeName;
    private final InflightMessageList inflightMessages;
    private Connection connection;
    private Channel channel;
    public RabbitmqProducer(MaxwellContext context) {
        super(context);
        this.inflightMessages = new InflightMessageList();
        this.exchangeName = context.getConfig().rabbitmqExchange;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(context.getConfig().rabbitmqHost);
        try {
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();
            this.channel.exchangeDeclare(exchangeName, context.getConfig().rabbitmqExchangeType);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
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
        System.out.println(" [x] Sent '" + value + "'");
    }
}
