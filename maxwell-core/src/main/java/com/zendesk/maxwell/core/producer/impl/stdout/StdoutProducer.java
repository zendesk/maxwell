package com.zendesk.maxwell.core.producer.impl.stdout;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.api.producer.AbstractProducer;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);

		if ( output != null )
			System.out.println(output);

		this.context.setPosition(r);
	}
}
