package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

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