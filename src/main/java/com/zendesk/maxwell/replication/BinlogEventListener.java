package com.zendesk.maxwell.replication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.code.or.binlog.BinlogEventV4;

class BinlogEventListener implements com.google.code.or.binlog.BinlogEventListener {
	private final BlockingQueue<BinlogEventV4> queue;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);

	public BinlogEventListener(BlockingQueue<BinlogEventV4> q) {
		this.queue = q;
	}
	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvents(BinlogEventV4 event) {
		while (mustStop.get() != true) {
			try {
				if ( queue.offer(event, 100, TimeUnit.MILLISECONDS ) ) {
					return;
				}
			} catch (InterruptedException e) { }
		}
	}
}
