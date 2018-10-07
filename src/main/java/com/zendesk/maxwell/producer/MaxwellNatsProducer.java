package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.streaming.AckHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.StreamingConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;

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
			return;
		}
		cc.markCompleted();
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


/**
 * Nats Streaming producer
 */
public class MaxwellNatsProducer  extends AbstractProducer {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellNatsProducer.class);

	private final ArrayBlockingQueue<RowMap> queue;
	private MaxwellNatsProducerWorker worker;

	public MaxwellNatsProducer(MaxwellContext context, String natsSubject, String clusterId, String clientId) {
		super(context);

		this.queue = new ArrayBlockingQueue<>(1000);
		try {
			this.worker = new MaxwellNatsProducerWorker(context, natsSubject, clusterId, clientId, queue);
		} catch (Exception e) {
			this.context.terminate(new RuntimeException(e));
		}
		Thread thread = new Thread(this.worker, "maxwell-nats-worker");
		thread.setDaemon(true);
		thread.start();
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.queue.put(r);
	}
}

class MaxwellNatsProducerWorker extends AbstractAsyncProducer implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellNatsProducer.class);

	private final String natsSubject;

	private Connection natsConnection;
	private NatsConnectionListener connectionListener;
	private StreamingConnection streamingConnection;
	private final ArrayBlockingQueue<RowMap> queue;
	private Thread thread;


	public MaxwellNatsProducerWorker(MaxwellContext context, String natsSubject, String clusterId,
					 String clientId, ArrayBlockingQueue<RowMap> queue) throws IOException, InterruptedException {
		super(context);

		this.natsSubject = natsSubject;
		this.queue = queue;
		this.connectionListener = new NatsConnectionListener(context);

		String natsUrl = context.getConfig().natsUrl;
		Long pubAckTimeout = context.getConfig().natsPubAckTimeout;

		Options.Builder clientOptions = new Options.Builder();
		io.nats.streaming.Options.Builder streamingOptions = new io.nats.streaming.Options.Builder();

		clientOptions
			.server(natsUrl)
			.connectionListener(connectionListener);

		if (context.getConfig().natsMaxReconnects > 0) {
			clientOptions.maxReconnects(context.getConfig().natsMaxReconnects);
		}

		if (context.getConfig().natsReconnectWait != null) {
			clientOptions.reconnectWait(Duration.ofMillis(context.getConfig().natsReconnectWait));
		}

		this.natsConnection = Nats.connect(clientOptions.build());
		streamingOptions.natsConn(natsConnection);

		if (pubAckTimeout != null) {
			streamingOptions.pubAckWait(Duration.ofMillis(pubAckTimeout));
		}

		this.streamingConnection = NatsStreaming.connect(clusterId, clientId, streamingOptions.build());

	}

	@Override
	public void run() {
		this.thread = Thread.currentThread();
		while ( true ) {
			try {
				//Irrespective of Nats is working or not, heartbeat should be pushed
				if (queue.peek() instanceof HeartbeatRowMap) {
					RowMap row = queue.take();
					this.push(row);
				} else if (connectionListener.isNatsRunning()){
					RowMap row = queue.take();
					this.push(row);
				}

			} catch ( Exception e ) {
				LOGGER.info("Error: ", e);
			}
		}
	}

	@Override
	public void sendAsync(RowMap r, AbstractAsyncProducer.CallbackCompleter cc) throws Exception {
		String msg = r.toJSON(outputConfig);
		NatsAckHandler handler = new NatsAckHandler(cc, r.getNextPosition(), msg,
			this.succeededMessageCount, this.failedMessageCount, this.succeededMessageMeter, this.failedMessageMeter, this.context);
		streamingConnection.publish(natsSubject, msg.getBytes(), handler);
	}

}

class NatsConnectionListener implements ConnectionListener {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellNatsProducer.class);
	private Boolean isNatsRunning = true;
	private final MaxwellContext context;

	public NatsConnectionListener(MaxwellContext context) {
		this.context = context;
	}

	@Override
	public void connectionEvent(Connection connection, Events events) {
		switch (events) {
			case DISCONNECTED:
				isNatsRunning = false;
				LOGGER.error("Nats connection disconnected");
				if (!context.getConfig().ignoreProducerError) {
					this.context.terminate(new RuntimeException("Nats Disconnected"));
					return;
				}
				break;
			case CLOSED:
				isNatsRunning = false;
				LOGGER.error("Nats connection closed");
				break;
			case RECONNECTED:
				isNatsRunning = true;
				LOGGER.info("Nats connection reconnected");
				break;
		}
	}

	public boolean isNatsRunning() {
		return isNatsRunning;
	}
}

