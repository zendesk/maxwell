package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ProfilerProducer extends AbstractProducer {
	private long count;
	private long startTime;
	private FileOutputStream nullOutputStream;

	public ProfilerProducer(MaxwellContext context) {
		super(context);
		this.count = 0;
		this.startTime = 0;
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		if ( this.nullOutputStream == null ) {
			this.nullOutputStream = new FileOutputStream(new File("/dev/null"));
		}

		if ( this.startTime == 0)
			this.startTime = System.currentTimeMillis();

		for ( String json : e.toJSONStrings() ) {
			nullOutputStream.write(json.getBytes());

			this.count++;
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

		this.context.setPosition(e);
	}
}
