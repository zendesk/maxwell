package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.BinlogConnectorEvent;
import com.zendesk.maxwell.row.RowMap;
import org.graylog2.gelfclient.*;
import org.graylog2.gelfclient.transport.GelfTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;


public class GraylogProdcuer extends AbstractProducer {

	public static final String COLUMN = "column";
	public static final String BEFORE = "before";
	public static final String AFTER = "after";

	private static final Logger logger = LoggerFactory.getLogger(GraylogProdcuer.class);

	private final GelfTransport transport;

	public GraylogProdcuer(MaxwellContext context) {
		this(context, createTransport(context.getConfig()));
	}

	GraylogProdcuer(MaxwellContext context, GelfTransport transport) {
		super(context);
		this.transport = transport;
	}

	@Override
	public void push(RowMap r) throws Exception {
		if (!r.shouldOutput(outputConfig)) {
			context.setPosition(r.getPosition());
			return;
		}

		try {
			switch (r.getRowType()) {
				case BinlogConnectorEvent.INSERT:
					sendInsertMessage(context, r);
					break;
				case BinlogConnectorEvent.UPDATE:
					sendUpdateMessage(context, r);
					break;
				case BinlogConnectorEvent.DELETE:
					sendDeleteMessage(context, r);
					break;
			}
		} catch (Exception e) {
			logger.error("Exception during send", e);

			if (!context.getConfig().ignoreProducerError) {
				throw new RuntimeException(e);
			}
		}
		if (r.isTXCommit()) {
			context.setPosition(r.getPosition());
		}
	}

	private static GelfMessageBuilder createGelfMessageBuilder(MaxwellContext context, RowMap r) {
		GelfMessageBuilder builder = new GelfMessageBuilder("Binlog event").level(GelfMessageLevel.INFO);

		Properties additionalField = context.getConfig().graylogAdditionalField;

		for (Map.Entry<Object, Object> e : additionalField.entrySet()) {
			String key = (String) e.getKey();
			String value = (String) e.getValue();
			builder.additionalField(key, value);
		}

		builder.additionalField("database", r.getDatabase());
		builder.additionalField("table", r.getTable());
		builder.additionalField("type", r.getRowType());
		builder.additionalField("server_id", r.getServerId());
		builder.additionalField("thread_id", r.getThreadId());
		builder.additionalField("xid", r.getXid());
		builder.timestamp(r.getTimestampMillis());

		return builder;
	}

	private static GelfTransport createTransport(MaxwellConfig config) {

		final GelfConfiguration configuration = new GelfConfiguration(new InetSocketAddress(config.graylogHost, config.graylogPort))
				.transport(GelfTransports.valueOf(config.graylogTransport.toUpperCase()))
				.queueSize(512)
				.connectTimeout(5000)
				.reconnectDelay(1000)
				.tcpNoDelay(true)
				.sendBufferSize(32768);

		return GelfTransports.create(configuration);
	}

	private void sendInsertMessage(MaxwellContext context, RowMap r) {
		LinkedHashMap<String, Object> data = r.getData();
		for (Map.Entry<String, Object> item : data.entrySet()) {
			GelfMessageBuilder builder = createGelfMessageBuilder(context, r);
			builder.additionalField(COLUMN, item.getKey());
			builder.additionalField(BEFORE, "");
			builder.additionalField(AFTER, item.getValue());

			send(builder);
		}
	}

	private void sendUpdateMessage(MaxwellContext context, RowMap r) {
		LinkedHashMap<String, Object> oldData = r.getOldData();
		for (Map.Entry<String, Object> item : oldData.entrySet()) {
			GelfMessageBuilder builder = createGelfMessageBuilder(context, r);
			builder.additionalField(COLUMN, item.getKey());
			builder.additionalField(AFTER, r.getData(item.getKey()));
			builder.additionalField(BEFORE, item.getValue());

			send(builder);
		}
	}

	private void sendDeleteMessage(MaxwellContext context, RowMap r) {
		LinkedHashMap<String, Object> data = r.getData();
		for (Map.Entry<String, Object> item : data.entrySet()) {
			GelfMessageBuilder builder = createGelfMessageBuilder(context, r);
			builder.additionalField(COLUMN, item.getKey());
			builder.additionalField(BEFORE, item.getValue());
			builder.additionalField(AFTER, "");

			send(builder);
		}
	}

	public void send(GelfMessageBuilder builder)
	{
		GelfMessage gelfMessage = builder.build();

		if (logger.isDebugEnabled()) {
			logger.debug("Send to graylog -> " + gelfMessage.toString());
		}

		transport.trySend(gelfMessage);
	}
}
