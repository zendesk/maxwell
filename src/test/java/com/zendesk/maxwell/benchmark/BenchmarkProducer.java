package com.zendesk.maxwell.benchmark;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProfilingProducer;
import com.zendesk.maxwell.row.RowMap;

public class BenchmarkProducer extends AbstractProfilingProducer {
	private long lastRowReceivedAt = 0;
	public BenchmarkProducer(MaxwellContext context) {
		super(context);

		new Thread(() -> {
			while (true) {
				checkDone();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) { }
			}

		}).start();
	}

	// crude, but should work - wait until there's no rows coming in and log.
	private void checkDone() {
		String msg;
		long now = System.currentTimeMillis();
		if ( lastRowReceivedAt > 0 && now - lastRowReceivedAt > 2000 )	 {
			double seconds = (lastRowReceivedAt - this.startTime) / 1000.0d;
			msg = String.format("Producer received %d rows in %.2f seconds", this.count, seconds );
			System.out.println(msg);
			msg = String.format("%.0f rows per second", this.count / seconds);
			System.out.println(msg);
			System.exit(0);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		super.push(r);
		this.lastRowReceivedAt = System.currentTimeMillis();
	}
}
