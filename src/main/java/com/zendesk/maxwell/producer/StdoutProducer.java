package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

public class StdoutProducer extends AbstractProducer {
	public StdoutProducer(MaxwellContext context) {
		super(context);
	}

	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);

		if ( output != null ) {
            String result = String.format("Table %s, database %s, event type is %s", r.getTable(), r.getDatabase(), r.getRowType());
			System.out.println(result);

        }

		this.context.setPosition(r);
	}
}
