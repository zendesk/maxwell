package com.zendesk.maxwell.replication;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
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
	private final MaxwellOutputConfig outputConfig;
	private long replicationLag;
	private String gtid;

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		Metrics metrics,
		MaxwellOutputConfig outputConfig
	) {
		this.client = client;
		this.queue = q;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));
		this.outputConfig = outputConfig;

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

		EventType eventType = event.getHeader().getEventType();

		// binlog_transaction_compression=ON delivers a transaction's events compressed inside a
		// single TRANSACTION_PAYLOAD event. The binlog client never unwraps it, so surface each
		// inner event (TABLE_MAP / rows / XID ...) through the normal path here. Inner events carry
		// positions relative to the in-memory decompressed buffer, not real binlog offsets; re-stamp
		// each with the payload event's on-disk position so downstream position tracking stays correct
		// (getPosition() is derived as nextPosition - eventLength, so we set both fields to the
		// payload's values, giving every inner event the payload's position and nextPosition).
		if ( eventType == EventType.TRANSACTION_PAYLOAD ) {
			EventHeaderV4 payloadHeader = event.getHeader();
			TransactionPayloadEventData payloadData = event.getData();
			for ( Event innerEvent : payloadData.getUncompressedEvents() ) {
				EventHeaderV4 innerHeader = innerEvent.getHeader();
				innerHeader.setEventLength(payloadHeader.getEventLength());
				innerHeader.setNextPosition(payloadHeader.getNextPosition());
				onEvent(innerEvent);
			}
			return;
		}

		if ( eventType == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		} else if ( eventType == EventType.MARIADB_GTID) {
			gtid = ((MariadbGtidEventData)event.getData()).toString();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid, outputConfig);

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

