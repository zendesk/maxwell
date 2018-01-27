package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 8/27/16.
 */
public class BufferedMaxwell extends Maxwell {
	public BufferedMaxwell(MaxwellConfig config) throws SQLException, URISyntaxException {
		super(config);
		config.producerType = "buffer";
	}

	public RowMap poll(long ms) throws IOException, InterruptedException {
		BufferedProducer p = (BufferedProducer) this.context.getProducer();
		return p.poll(ms, TimeUnit.MILLISECONDS);
	}

	public MaxwellContext getContext() {
		return context;
	}
}
