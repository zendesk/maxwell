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

		public CallbackCompleter(InflightMessageList inflightMessages, BinlogPosition position, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.position = position;
			this.isTXCommit = isTXCommit;
		}

		public void markCompleted() {
			if(isTXCommit) {
				BinlogPosition newPosition = inflightMessages.completeMessage(position);

				if(newPosition != null) {
					context.setPosition(newPosition);
				}
			}
		}
	}

	private InflightMessageList inflightMessages;

	public AbstractAsyncProducer(MaxwellContext context) {
		super(context);

		this.inflightMessages = new InflightMessageList();
	}

	public abstract void push(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public final void push(RowMap r) throws Exception {
		String value = r.toJSON(outputConfig);

		if(value == null) { // heartbeat row or other row with suppressed output
			inflightMessages.addMessage(r.getPosition());
			BinlogPosition newPosition = inflightMessages.completeMessage(r.getPosition());

			if(newPosition != null) {
				context.setPosition(newPosition);
			}

			return;
		}

		// release reference to ease memory pressure
		value = null;

		if(r.isTXCommit()) {
			inflightMessages.addMessage(r.getPosition());
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, r.getPosition(), r.isTXCommit(), context);

		push(r, cc);
	}
}
