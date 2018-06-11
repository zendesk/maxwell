package com.zendesk.maxwell.replication;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.zendesk.maxwell.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventListener.class);

	private final BlockingQueue<BinlogConnectorEvent> queue;
	private final Timer queueTimer;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private long replicationLag;
	private String gtid;

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		Metrics metrics) {
		this.client = client;
		this.queue = q;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));

		final BinlogConnectorEventListener self = this;
		metrics.register(metrics.metricName("replication", "lag"), (Gauge<Long>) () -> self.replicationLag);
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		long eventSeenAt = 0;
		boolean trackMetrics = false;

		if (event.getHeader().getEventType() == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);

		if (ep.isCommitEvent()) {
			trackMetrics = true;
			eventSeenAt = System.currentTimeMillis();
			replicationLag = eventSeenAt - event.getHeader().getTimestamp();
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
}

