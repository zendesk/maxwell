package com.zendesk.maxwell.core;

import com.zendesk.maxwell.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.producer.BufferedProducer;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.row.RowMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 8/27/16.
 */
public class BufferedMaxwellRunner extends MaxwellRunner {
	public BufferedMaxwellRunner(MaxwellConfig config) throws SQLException, URISyntaxException {
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
