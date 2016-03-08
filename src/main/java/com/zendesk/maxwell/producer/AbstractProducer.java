package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.filter.MaxwellColumnFilter;

public abstract class AbstractProducer {
	protected final MaxwellContext context;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
	}

	abstract public void push(RowMap r) throws Exception;
}
