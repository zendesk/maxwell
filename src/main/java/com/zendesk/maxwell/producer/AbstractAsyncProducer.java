package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.metrics.MaxwellMetrics;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.metrics.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

public abstract class AbstractAsyncProducer extends AbstractProducer {

	protected final Counter succeededMessageCount;
	protected final Meter succeededMessageMeter;
	protected final Counter failedMessageCount;
	protected final Meter failedMessageMeter;

	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final Position position;
		private final boolean isTXCommit;
		private final long sendTimeMS;
		private Long completeTimeMS;

		public CallbackCompleter(InflightMessageList inflightMessages, Position position, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.position = position;
			this.isTXCommit = isTXCommit;
			this.sendTimeMS = System.currentTimeMillis();
		}

		public void markCompleted() {
			if(isTXCommit) {
				Position newPosition = inflightMessages.completeMessage(position);

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

		Metrics metrics = context.getMetrics();
		MetricRegistry metricRegistry = metrics.getRegistry();

		String gaugeName = metrics.metricName("inflightmessages", "count");
		metricRegistry.register(
			gaugeName,
			new Gauge<Long>() {
				@Override
				public Long getValue() {
					return (long) inflightMessages.size();
				}
			}
		);

		this.succeededMessageCount = metricRegistry.counter(metrics.metricName("messages", "succeeded"));
		this.succeededMessageMeter = metricRegistry.meter(metrics.metricName("messages", "succeeded", "meter"));
		this.failedMessageCount = metricRegistry.counter(metrics.metricName("messages", "failed"));
		this.failedMessageMeter = metricRegistry.meter(metrics.metricName("messages", "failed", "meter"));
	}

	public abstract void sendAsync(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public Meter getFailedMessageMeter() {
		return this.failedMessageMeter;
	}

	@Override
	public final void push(RowMap r) throws Exception {
		Position position = r.getPosition();
		// Rows that do not get sent to a target will be automatically marked as complete.
		// We will attempt to commit a checkpoint up to the current row.
		if(!r.shouldOutput(outputConfig)) {
			inflightMessages.addMessage(position);

			Position completed = inflightMessages.completeMessage(position);
			if(completed != null) {
				context.setPosition(completed);
			}
			return;
		}

		if(r.isTXCommit()) {
			inflightMessages.addMessage(position);
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, position, r.isTXCommit(), context);

		sendAsync(r, cc);
	}
}
