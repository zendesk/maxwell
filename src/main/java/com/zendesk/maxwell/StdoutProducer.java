package com.zendesk.maxwell;

import com.zendesk.maxwell.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellConfig config) {
		super(config);
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		for ( String json : e.toJSONStrings() ) {
			System.out.println(json);
		}
		this.config.setInitialPosition(e.getNextBinlogPosition());
	}
}
