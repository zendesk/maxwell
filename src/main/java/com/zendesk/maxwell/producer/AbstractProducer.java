package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;

public abstract class AbstractProducer {
	protected final MaxwellContext context;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
	}
	abstract public void push(MaxwellAbstractRowsEvent e) throws Exception;

	public void onComplete(MaxwellAbstractRowsEvent e) {
		System.out.println("processed " + e.getBinlogFilename() + ":" + e.getHeader().getPosition());
	}
}
