package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellOutputConfig;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.core.monitoring.Metrics;
import com.zendesk.maxwell.core.util.StoppableTask;

public abstract class AbstractProducer implements Producer {
	protected final MaxwellContext context;
	protected final MaxwellOutputConfig outputConfig;
	protected final Counter succeededMessageCount;
	protected final Meter succeededMessageMeter;
	protected final Counter failedMessageCount;
	protected final Meter failedMessageMeter;
	protected final Timer metricsTimer;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().getOutputConfig();

		Metrics metrics = context.getMetrics();
		MetricRegistry metricRegistry = metrics.getRegistry();

		this.succeededMessageCount = metricRegistry.counter(metrics.metricName("messages", "succeeded"));
		this.succeededMessageMeter = metricRegistry.meter(metrics.metricName("messages", "succeeded", "meter"));
		this.failedMessageCount = metricRegistry.counter(metrics.metricName("messages", "failed"));
		this.failedMessageMeter = metricRegistry.meter(metrics.metricName("messages", "failed", "meter"));
		this.metricsTimer = metrics.getRegistry().timer(metrics.metricName("message", "publish", "time"));
	}

	public StoppableTask getStoppableTask() {
		return null;
	}

	@Override
	public Meter getFailedMessageMeter() {
		return this.failedMessageMeter;
	}

	@Override
	public MaxwellDiagnostic getDiagnostic() {
		return null;
	}
}
