package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;

public abstract class AbstractProducer {
	protected final MaxwellContext context;
	protected final MaxwellOutputConfig outputConfig;
	protected final Counter succeededMessageCount;
	protected final Meter succeededMessageMeter;
	protected final Counter failedMessageCount;
	protected final Meter failedMessageMeter;
	protected final Timer messagePublishTimer;
	protected final Timer messageLatencyTimer;
	protected final Counter messageLatencySloViolationCount;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().outputConfig;

		Metrics metrics = context.getMetrics();
		MetricRegistry metricRegistry = metrics.getRegistry();

		this.succeededMessageCount = metricRegistry.counter(metrics.metricName("messages", "succeeded"));
		this.succeededMessageMeter = metricRegistry.meter(metrics.metricName("messages", "succeeded", "meter"));
		this.failedMessageCount = metricRegistry.counter(metrics.metricName("messages", "failed"));
		this.failedMessageMeter = metricRegistry.meter(metrics.metricName("messages", "failed", "meter"));
		this.messagePublishTimer = metricRegistry.timer(metrics.metricName("message", "publish", "time"));
		this.messageLatencyTimer = metricRegistry.timer(metrics.metricName("message", "publish", "age"));
		this.messageLatencySloViolationCount = metricRegistry.counter(metrics.metricName("message", "publish", "age", "slo_violation"));
	}

	abstract public void push(RowMap r) throws Exception;

	public StoppableTask getStoppableTask() {
		return null;
	}

	public Meter getFailedMessageMeter() {
		return this.failedMessageMeter;
	}

	public MaxwellDiagnostic getDiagnostic() {
		return null;
	}
}
