package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import io.nats.streaming.AckHandler;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.StreamingConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

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
		Long pubAckTimeout = context.getConfig().natsPubAckTimeout;
		StreamingConnectionFactory cf = new StreamingConnectionFactory(context.getConfig().natsClusterId, context.getConfig().natsClientId);
		cf.setNatsUrl(context.getConfig().natsUrl);
		if (pubAckTimeout != null) {
			cf.setAckTimeout(Duration.ofMillis(pubAckTimeout));
		}
		try {
			this.connection = cf.createConnection();
		} catch (IOException | InterruptedException e) {
			logger.error("Exception during Nats Connection", e);
			this.context.terminate(new RuntimeException(e));
		}
	}


	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String msg = r.toJSON(outputConfig);
		NatsAckHandler handler = new NatsAckHandler(cc, r.getNextPosition(), msg,
			this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);
		connection.publish(natsSubject, msg.getBytes(), handler);
	}

}

class NatsAckHandler implements AckHandler {
	public static final Logger logger = LoggerFactory.getLogger(NatsAckHandler.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;

	private Counter succeededMessageCount;
	private Counter failedMessageCount;
	private Meter succeededMessageMeter;
	private Meter failedMessageMeter;

	NatsAckHandler(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
		       Counter producedMessageCount, Counter failedMessageCount, Meter producedMessageMeter,
		       Meter failedMessageMeter, MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.succeededMessageCount = producedMessageCount;
		this.failedMessageCount = failedMessageCount;
		this.succeededMessageMeter = producedMessageMeter;
		this.failedMessageMeter = failedMessageMeter;
		this.context = context;
	}

	@Override
	public void onAck(String guid, Exception e) {
		if (e == null) {
			onSuccess(guid);
		} else {
			onError(e);
		}
	}

	private void onError(Exception err) {
		this.failedMessageCount.inc();
		this.failedMessageMeter.mark();

		logger.error(err.getClass().getSimpleName() + " @ " + position + " -- ");
		logger.error(err.getLocalizedMessage());
		logger.error("Exception during put", err);

		if (!context.getConfig().ignoreProducerError) {
			this.context.terminate(new RuntimeException(err));
		} else {
			cc.markCompleted();
		}
	}

	private void onSuccess(String guid) {
		this.succeededMessageCount.inc();
		this.succeededMessageMeter.mark();

		if (logger.isDebugEnabled()) {
			logger.debug("-> Nats Message id:" + guid + "  " + json + "  " + position);
		}
		cc.markCompleted();
	}

}
