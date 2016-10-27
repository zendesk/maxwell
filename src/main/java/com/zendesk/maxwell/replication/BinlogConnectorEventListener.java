package com.zendesk.maxwell.replication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.google.code.or.binlog.BinlogEventV4;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener {
	private final BlockingQueue<EventWithPosition> queue;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;

	public BinlogConnectorEventListener(BinaryLogClient client, BlockingQueue<EventWithPosition> q) {
		this.client = client;
		this.queue = q;
	}
	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		while (mustStop.get() != true) {
			EventWithPosition ep = new EventWithPosition(event, BinlogPosition.at(client.getBinlogPosition(), client.getBinlogFilename()));
			try {
				if ( queue.offer(ep, 100, TimeUnit.MILLISECONDS ) ) {
					return;
				}
			} catch (InterruptedException e) { }
		}
	}
}

