package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellConfig config) {
		super(config);
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		System.out.println(e.toJSON());
		this.config.setInitialPosition(e.getNextBinlogPosition());
	}
}
