package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellConfig;

public abstract class AbstractProducer {
	protected final MaxwellConfig config;

	public AbstractProducer(MaxwellConfig config) {
		this.config = config;
	}
	abstract public void push(MaxwellAbstractRowsEvent e) throws Exception;

	public void onComplete(MaxwellAbstractRowsEvent e) {
		System.out.println("processed " + e.getBinlogFilename() + ":" + e.getHeader().getPosition());
	}
}
