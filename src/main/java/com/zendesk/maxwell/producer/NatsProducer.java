package com.zendesk.maxwell.producer;

import io.nats.client.Connection;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NatsProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(NatsProducer.class);
	private final Connection natsConnection;
	private final String natsSubjectPrefix;
	private final String subjectHierarchiesTemplate;

	public NatsProducer(MaxwellContext context) {
		super(context);
		try {
			this.natsConnection = Nats.connect(context.getConfig().natsUrl);
			final String natsPrefix = context.getConfig().natsSubjectPrefix;
			this.natsSubjectPrefix = natsPrefix.equals("") ? "": natsPrefix + "." ;
			this.subjectHierarchiesTemplate = context.getConfig().natsSubjectHierarchies;

		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());

			return;
		}

		String value = r.toJSON(outputConfig);
		String subjectHierarchies = getSubjectHierarchies(r);
		String natsSubject = natsSubjectPrefix + subjectHierarchies;

		natsConnection.publish(natsSubject, value.getBytes());
		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  nats subject:" + natsSubject + ", message:" + value);
		}
	}

	private String getSubjectHierarchies(RowMap r) {
		String table = r.getTable();

		if ( table == null )
			table = "";

		String type = r.getRowType();

		if ( type == null )
			type = "";

		return subjectHierarchiesTemplate
				.replace("%db%", r.getDatabase())
				.replace("%table%", table)
				.replace("%type%", type);
	}
}
