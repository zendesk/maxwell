package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.TimeUnit;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);

		if ( output != null && r.shouldOutput(outputConfig) )
			System.out.println(output);

		this.messageLatencyTimer.update(
			System.currentTimeMillis() - r.getTimestampMillis(),
			TimeUnit.MILLISECONDS
		);
		this.context.setPosition(r);
	}
}
