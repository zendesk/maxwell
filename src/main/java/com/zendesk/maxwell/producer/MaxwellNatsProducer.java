package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import io.nats.streaming.AckHandler;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Nats Streaming producer
 */
public class MaxwellNatsProducer extends AbstractAsyncProducer {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellNatsProducer.class);
	private final String natsSubject;
	private StreamingConnection connection;

	public MaxwellNatsProducer(MaxwellContext context, String natsSubject) {
		super(context);
		this.natsSubject = natsSubject;
		StreamingConnectionFactory cf = new StreamingConnectionFactory(context.getConfig().natsClusterId, context.getConfig().natsClientId);
		cf.setNatsUrl(context.getConfig().natsUrl);
		try {
			this.connection = cf.createConnection();
		} catch (IOException | InterruptedException e) {
			logger.error("Exception during Nats Connection", e);
		}
	}


	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String msg = r.toJSON();
		NatsAckHandler handler = new NatsAckHandler(cc, r.getNextPosition(), msg, context);
		connection.publish(natsSubject, msg.getBytes(), handler);
	}

}

class NatsAckHandler implements AckHandler {
	public static final Logger logger = LoggerFactory.getLogger(NatsAckHandler.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;

	NatsAckHandler(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
		       MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.context = context;
	}

	@Override
	public void onAck(String guid, Exception e) {
		if (e != null) {
			onError(e);
		} else {
			onSuccess(guid);
		}
	}

	private void onError(Exception err) {
		logger.error(err.getClass().getSimpleName() + " @ " + position + " -- ");
		logger.error(err.getLocalizedMessage());
		logger.error("Exception during put", err);

		if (!context.getConfig().ignoreProducerError) {
			context.terminate(new RuntimeException(err));
		} else {
			cc.markCompleted();
		}
	}

	private void onSuccess(String guid) {
		if (logger.isDebugEnabled()) {
			logger.debug("-> Nats Message id:" + guid + "  " + json + "  " + position);
		}
		cc.markCompleted();
	}

}
