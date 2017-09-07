package com.zendesk.maxwell.replication;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.zendesk.maxwell.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener,
					      BinaryLogClient.LifecycleListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventListener.class);

	private final BlockingQueue<BinlogConnectorEvent> queue;
	private final Timer queueTimer;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private String gtid;
	private long eventSeenAt;
	private boolean trackMetrics;
	private final Histogram replicationLag;

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		Metrics metrics) {
		this.client = client;
		this.queue = q;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));
		this.replicationLag = metrics.getRegistry().histogram(metrics.metricName("replication", "lag"));
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		EventType eventType = event.getHeader().getEventType();

		if (eventType == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);

		if (ep.isCommitEvent()) {
			trackMetrics = true;
			eventSeenAt = System.currentTimeMillis();
			replicationLag.update(eventSeenAt - event.getHeader().getTimestamp());
		} else {
			trackMetrics = false;
		}

		while (mustStop.get() != true) {
			try {
				if ( queue.offer(ep, 100, TimeUnit.MILLISECONDS ) ) {
					break;
				}
			} catch (InterruptedException e) {
				return;
			}
		}

		if (trackMetrics) {
			queueTimer.update(System.currentTimeMillis() - eventSeenAt, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void onConnect(BinaryLogClient client) {
		LOGGER.info("Binlog connected.");
	};

	@Override
	public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("Communication failure.", ex);
	}

	@Override
	public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
		LOGGER.warn("Event deserialization failure.", ex);
	}

	@Override
	public void onDisconnect(BinaryLogClient client) {
		LOGGER.info("Binlog disconnected.");
	}
}

