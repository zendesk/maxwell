package com.zendesk.maxwell.producer;

import com.codahale.metrics.Gauge;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.TimeUnit;

public abstract class AbstractAsyncProducer extends AbstractProducer {

	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final MaxwellConfig config;
		private final Position position;
		private final boolean isTXCommit;
		private final long messageID;

		public CallbackCompleter(InflightMessageList inflightMessages, Position position, boolean isTXCommit, MaxwellContext context, long messageID) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.config = context.getConfig();
			this.position = position;
			this.isTXCommit = isTXCommit;
			this.messageID = messageID;
		}

		public void markCompleted() {
			inflightMessages.freeSlot(messageID);
			if(isTXCommit) {
				InflightMessageList.InflightMessage message = inflightMessages.completeMessage(position);

				if (message != null) {
					context.setPosition(message.position);
					long currentTime = System.currentTimeMillis();
					long age = currentTime - message.sendTimeMS;

					messagePublishTimer.update(age, TimeUnit.MILLISECONDS);
					messageLatencyTimer.update(Math.max(0L, age - 500L), TimeUnit.MILLISECONDS);

					if (age > config.metricsAgeSlo) {
						messageLatencySloViolationCount.inc();
					}
				}
			}
		}
	}

	private InflightMessageList inflightMessages;

	public AbstractAsyncProducer(MaxwellContext context) {
		super(context);

		this.inflightMessages = new InflightMessageList(context);

		Metrics metrics = context.getMetrics();
		String gaugeName = metrics.metricName("inflightmessages", "count");
		metrics.register(gaugeName, (Gauge<Long>) () -> (long) inflightMessages.size());
	}

	public abstract void sendAsync(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public final void push(RowMap r) throws Exception {
		Position position = r.getNextPosition();
		// Rows that do not get sent to a target will be automatically marked as complete.
		// We will attempt to commit a checkpoint up to the current row.
		if(!r.shouldOutput(outputConfig)) {
			if ( position != null ) {
				inflightMessages.addMessage(position, r.getTimestampMillis(), 0L);

				InflightMessageList.InflightMessage completed = inflightMessages.completeMessage(position);
				if (completed != null) {
					context.setPosition(completed.position);
				}
			}
			return;
		}

		// back-pressure from slow producers

		long messageID = inflightMessages.waitForSlot();

		if(r.isTXCommit()) {
			inflightMessages.addMessage(position, r.getTimestampMillis(), messageID);
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, position, r.isTXCommit(), context, messageID);

		sendAsync(r, cc);
	}
}
