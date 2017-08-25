package com.zendesk.maxwell.replication;

import com.codahale.metrics.Gauge;
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
	private Long executionTime = 0L;
	private Long replicationLag = 0L;
	private long transactionStartTime = 0L;

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		Metrics metrics) {
		this.client = client;
		this.queue = q;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));

		final BinlogConnectorEventListener self = this;

		String executionTimeGaugeName = metrics.metricName("mysql", "transaction", "execution", "time");
		metrics.getRegistry().register(
			executionTimeGaugeName,
			new Gauge<Long>() {
				@Override
				public Long getValue() {
					return self.executionTime;
				}
			}
		);

		String lagGaugeName = metrics.metricName("replication", "lag");
		metrics.getRegistry().register(
			lagGaugeName,
			new Gauge<Long>() {
				@Override
				public Long getValue() {
					return self.replicationLag;
				}
			}
		);
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		long eventSeenAt = 0L;
		boolean trackMetrics = false;

		EventType eventType = event.getHeader().getEventType();

		if (eventType == EventType.QUERY) {
			transactionStartTime = event.getHeader().getTimestamp();
		} else if (eventType == EventType.XID) {
			trackMetrics = true;
			eventSeenAt = System.currentTimeMillis();
			long transactionCommitTime = event.getHeader().getTimestamp();
			replicationLag = eventSeenAt - transactionCommitTime;
			executionTime = transactionCommitTime - transactionStartTime;
		} else if (eventType == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);

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

