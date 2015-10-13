package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(MaxwellAbstractRowsEvent e) throws Exception {
		for ( String json : e.toJSONStrings() ) {
			System.out.println(json);
		}
		this.context.setPosition(e);
	}
}
