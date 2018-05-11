package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.io.File;
import java.io.FileOutputStream;

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
