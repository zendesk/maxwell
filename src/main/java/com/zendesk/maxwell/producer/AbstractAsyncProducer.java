package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

public abstract class AbstractAsyncProducer extends AbstractProducer {
	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final BinlogPosition position;
		private final boolean isTXCommit;
		private final long sendTimeMS;
		private Long completeTimeMS;

		public CallbackCompleter(InflightMessageList inflightMessages, BinlogPosition position, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.position = position;
			this.isTXCommit = isTXCommit;
			this.sendTimeMS = System.currentTimeMillis();
		}

		public void markCompleted() {
			if(isTXCommit) {
				BinlogPosition newPosition = inflightMessages.completeMessage(position);

				if(newPosition != null) {
					context.setPosition(newPosition);
				}
			}
			completeTimeMS = System.currentTimeMillis();
		}

		public Long timeToSendMS() {
			if ( completeTimeMS == null ) return null;
			return completeTimeMS - sendTimeMS;
		}
	}

	private InflightMessageList inflightMessages;

	public AbstractAsyncProducer(MaxwellContext context) {
		super(context);

		this.inflightMessages = new InflightMessageList();
	}

	public abstract void sendAsync(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public final void push(RowMap r) throws Exception {
		// Rows that do not get sent to a target will be automatically marked as complete.
		// We will attempt to commit a checkpoint up to the current row.
		if(!r.shouldOutput(outputConfig)) {
			inflightMessages.addMessage(r.getPosition());
			BinlogPosition newPosition = inflightMessages.completeMessage(r.getPosition());

			if(newPosition != null) {
				context.setPosition(newPosition);
			}

			return;
		}

		if(r.isTXCommit()) {
			inflightMessages.addMessage(r.getPosition());
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, r.getPosition(), r.isTXCommit(), context);

		sendAsync(r, cc);
	}
}
