package com.zendesk.maxwell.replication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private String gtid;

	public BinlogConnectorEventListener(BinaryLogClient client, BlockingQueue<BinlogConnectorEvent> q) {
		this.client = client;
		this.queue = q;
	}
	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		while (mustStop.get() != true) {
			if (event.getHeader().getEventType() == EventType.GTID) {
				gtid = ((GtidEventData)event.getData()).getGtid();
			}
			BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);
			try {
				if ( queue.offer(ep, 100, TimeUnit.MILLISECONDS ) ) {
					return;
				}
			} catch (InterruptedException e) { }
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

