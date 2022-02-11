package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * A subclass of {@link Maxwell} that buffers rows in-memory for consumption by the caller
 */
public class BufferedMaxwell extends Maxwell {
	/**
	 * Initializer for a buffered Maxwell instance.  Sets up buffered producer.
	 * @param config Maxwell configuration
	 * @throws SQLException if we have db issues
	 * @throws URISyntaxException if we can't build a database URI
	 */
	public BufferedMaxwell(MaxwellConfig config) throws SQLException, URISyntaxException {
		super(config);
		config.producerType = "buffer";
	}

	/**
	 * Poll for a RowMap to be producer by the maxwell instance.
	 * @param ms poll time to wait in milliseconds before timing out
	 * @return RowMap
	 * @throws IOException if we have issues building a producer
	 * @throws InterruptedException if we are interrupted
	 */
	public RowMap poll(long ms) throws IOException, InterruptedException {
		BufferedProducer p = (BufferedProducer) this.context.getProducer();
		return p.poll(ms, TimeUnit.MILLISECONDS);
	}

	public MaxwellContext getContext() {
		return context;
	}
}
