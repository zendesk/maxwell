package com.zendesk.maxwell.core.producer.impl.profiler;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.producer.AbstractProfilingProducer;
import com.zendesk.maxwell.core.row.RowMap;

public class ProfilerProducer extends AbstractProfilingProducer {
	public ProfilerProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		super.push(r);

		if ( this.count % 10000 == 0 ) {
			long elapsed = System.currentTimeMillis() - this.startTime;
			System.out.println("rows per second: " + (count * 1000) / elapsed);
		}

		if ( this.count % 1000000 == 0 ) {
			System.out.println("resetting statistics.");
			this.count = 0;
			this.startTime = System.currentTimeMillis();
		}
	}
}
