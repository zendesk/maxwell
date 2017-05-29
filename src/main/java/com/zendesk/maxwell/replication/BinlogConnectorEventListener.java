package com.zendesk.maxwell.replication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener,
					      BinaryLogClient.LifecycleListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventListener.class);

	private final BlockingQueue<BinlogConnectorEvent> queue;
	private Timer queueTimer;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private String gtid;

	private long replicationWait = 0L;
	private long lastProcessedEventTimestamp = 0L;
	private long lastProcessedEventAt;

	public BinlogConnectorEventListener(BinaryLogClient client, BlockingQueue<BinlogConnectorEvent> q, Timer queueTimer) {
		this.client = client;
		this.queue = q;
		this.queueTimer = queueTimer;
		this.lastProcessedEventAt = System.currentTimeMillis();
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		long eventSeenAt = 0L;
		long eventTimestamp = 0L;

		boolean trackMetrics = event.getHeader().getEventType() == EventType.XID;
		if (trackMetrics) {
			// replicationWait is not lag, but a measure of how much we're
			// waiting for events - if events come in with timestamp intervals smaller
			// than the clock time we spend waiting, the DB is slowing us down.
			eventSeenAt = System.currentTimeMillis();
			eventTimestamp = event.getHeader().getTimestamp();

			long eventTimeDiff = eventTimestamp - this.lastProcessedEventTimestamp;
			long clockTimeDiff = eventSeenAt - this.lastProcessedEventAt;
			this.replicationWait = Math.max(0L, this.replicationWait + (
				clockTimeDiff - eventTimeDiff
			));
		}

		while (mustStop.get() != true) {
			if (event.getHeader().getEventType() == EventType.GTID) {
				gtid = ((GtidEventData)event.getData()).getGtid();
			}

			BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);
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
			this.lastProcessedEventAt = eventSeenAt;
			this.lastProcessedEventTimestamp = eventTimestamp;
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

	public long getReplicationWait() {
		return replicationWait;
	}
}

