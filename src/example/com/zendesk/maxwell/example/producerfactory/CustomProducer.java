package com.zendesk.maxwell.example.producerfactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Custom {@link AbstractProducer} example that collects all the rows for a transaction and writes them to standard out.
 */
public class CustomProducer extends AbstractProducer {
	private final String headerFormat;
	private final Collection<RowMap> txRows = new ArrayList<>();

	public CustomProducer(MaxwellContext context) {
		super(context);
		// this property would be 'custom_producer.header_format' in config.properties
		headerFormat = context.getConfig().customProducerProperties.getProperty("header_format", "Transaction: %xid% >>>\n");
	}

	@Override
	public void push(RowMap r) throws Exception
	{
		// filtering out DDL and heartbeat rows
		if(!r.shouldOutput(outputConfig)) {
			context.setPosition(r.getPosition());
			return;
		}

		// custom producer logic here
		txRows.add(r);
		if(r.isTXCommit()) {
			System.out.print(headerFormat.replace("%xid%", r.getXid().toString()));
			txRows.stream()
				.map(CustomProducer::toJSON)
				.forEach(System.out::println);
			txRows.clear();
		}
	}
	
	private static String toJSON(RowMap row) {
		try {
			return row.toJSON();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
