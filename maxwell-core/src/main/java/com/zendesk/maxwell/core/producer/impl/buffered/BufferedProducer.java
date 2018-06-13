package com.zendesk.maxwell.core.producer.impl.buffered;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.producer.AbstractProducer;
import com.zendesk.maxwell.core.row.HeartbeatRowMap;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BufferedProducer extends AbstractProducer {
	private final LinkedBlockingQueue<RowMap> queue;

	public BufferedProducer(MaxwellContext context, int maxSize) {
		super(context);
		this.queue = new LinkedBlockingQueue<>(maxSize);
	}

	@Override
	public void push(RowMap r) throws Exception {
		// set position on heartbeats immediately to ensure we terminate cleanly
		if (r instanceof HeartbeatRowMap) {
			this.context.setPosition(r);
		}
		try {
			this.queue.put(r);
		} catch ( InterruptedException e ) {}
	}

	public RowMap poll(long timeout, TimeUnit unit) throws InterruptedException {
		RowMap r = this.queue.poll(timeout, unit);
		if (r != null) {
			this.context.setPosition(r);
		}
		return r;
	}
}
