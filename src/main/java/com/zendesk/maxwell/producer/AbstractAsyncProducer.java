package com.zendesk.maxwell.producer;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

public abstract class AbstractAsyncProducer extends AbstractProducer {
	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final int rowId;
		private final boolean isTXCommit;

		public CallbackCompleter(InflightMessageList inflightMessages, int rowId, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.rowId = rowId;
			this.isTXCommit = isTXCommit;
		}

		public void markCompleted() {
			if(isTXCommit) {
				BinlogPosition newPosition = inflightMessages.completeTXMessage(rowId);

				if(newPosition != null) {
					context.setPosition(newPosition);
				}
			} else {
				inflightMessages.completeNonTXMessage(rowId);
			}
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
		if(r.isTXCommit()) {
			inflightMessages.addTXMessage(r.getRowId(), r.getPosition());

			// Rows that do not get sent to a target will be automatically marked as complete.
			// We will attempt to commit a checkpoint up to the current row.
			if(!r.shouldOutput(outputConfig)) {
				BinlogPosition newPosition = inflightMessages.completeTXMessage(r.getRowId());

				if(newPosition != null) {
					context.setPosition(newPosition);
				}

				return;
			}
		} else {
			inflightMessages.addNonTXMessage(r.getRowId());
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, r.getRowId(), r.isTXCommit(), context);

		sendAsync(r, cc);
	}
}
