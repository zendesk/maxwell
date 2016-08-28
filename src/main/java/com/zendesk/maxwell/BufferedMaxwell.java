package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.BufferedProducer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 8/27/16.
 */
public class BufferedMaxwell extends Maxwell {
	public BufferedMaxwell(MaxwellConfig config) throws SQLException {
		super(config);
		config.producerType = "buffer";
	}

	public RowMap getRow(long timeout, TimeUnit unit) throws IOException, InterruptedException {
		BufferedProducer p = (BufferedProducer) this.context.getProducer();
		return p.poll(timeout, unit);
	}

}
