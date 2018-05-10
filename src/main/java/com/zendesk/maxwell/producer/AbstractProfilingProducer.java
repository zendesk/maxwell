package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.io.File;
import java.io.FileOutputStream;

public abstract class AbstractProfilingProducer extends AbstractProducer {
	protected long count;
	protected long startTime;
	private FileOutputStream nullOutputStream;

	public AbstractProfilingProducer(MaxwellContext context) {
		super(context);
		this.startTime = 0;
		this.count = 0;
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( this.nullOutputStream == null ) {
			this.nullOutputStream = new FileOutputStream(new File("/dev/null"));
		}

		if ( this.startTime == 0)
			this.startTime = System.currentTimeMillis();

		String value = r.toJSON();

		if (value != null) {
			nullOutputStream.write(value.getBytes());
		}

		this.count++;
		this.context.setPosition(r);
	}
}
