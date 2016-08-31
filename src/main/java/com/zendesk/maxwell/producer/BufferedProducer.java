package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

import java.sql.SQLException;
import java.util.ArrayList;
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
		this.queue.put(r);
	}

	@Override
	public void writePosition(BinlogPosition p) throws SQLException {
		RowMap fakePositionRow = new RowMap("heartbeat", "maxwell", "heartbeat", System.currentTimeMillis(), new ArrayList<String>(), p);
		try {
			this.queue.put(fakePositionRow);
		} catch ( InterruptedException e ) { }
	}

	public RowMap poll(long timeout, TimeUnit unit) throws InterruptedException {
		RowMap r = this.queue.poll(timeout, unit);
		if (r != null) {
			this.context.setPosition(r.getPosition());
		}
		return r;
	}
}
