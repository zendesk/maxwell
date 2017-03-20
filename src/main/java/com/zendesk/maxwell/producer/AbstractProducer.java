package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

public abstract class AbstractProducer {
	protected final MaxwellContext context;
	protected final MaxwellOutputConfig outputConfig;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().outputConfig;
	}

	abstract public void push(RowMap r) throws Exception;

	public void stop() throws IOException, TimeoutException { // should be abstract
		
	}
}
