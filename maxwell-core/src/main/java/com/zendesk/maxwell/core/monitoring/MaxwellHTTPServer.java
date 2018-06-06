package com.zendesk.maxwell.core.monitoring;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.core.ContextStartListener;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.config.MaxwellDiagnosticConfig;
import com.zendesk.maxwell.core.producer.Producers;
import com.zendesk.maxwell.core.util.StoppableTask;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

@Service
public class MaxwellHTTPServer implements ContextStartListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHTTPServer.class);

	private final Producers producers;

	@Autowired
	public MaxwellHTTPServer(Producers producers) {
		this.producers = producers;
	}

	@Override
	public void onContextStart(MaxwellContext context) {
		startIfRequired(context);
	}

	private void startIfRequired(MaxwellContext context) {
		boolean httpReportingEnabled = isHttpReportingEnabled(context);
		MaxwellDiagnosticContext diagnosticContext = getDiagnosticContext(context);
		if (httpReportingEnabled || diagnosticContext != null) {
			LOGGER.info("Maxwell http server starting");
			int port = context.getConfig().getHttpPort();
			String httpBindAddress = context.getConfig().getHttpBindAddress();
			String pathPrefix = context.getConfig().getHttpPathPrefix();
			context.getHealthCheckRegistry().register("MaxwellHealth", new MaxwellHealthCheck(context.getProducer()));
			MaxwellHTTPServerWorker maxwellHTTPServerWorker = new MaxwellHTTPServerWorker(httpBindAddress, port, pathPrefix, context.getMetricRegistry(), context.getHealthCheckRegistry(), diagnosticContext);
			Thread thread = new Thread(maxwellHTTPServerWorker);

			context.addTask(maxwellHTTPServerWorker);
			thread.setUncaughtExceptionHandler((t, e) -> {
				LOGGER.error("Maxwell http server failure", e);
				context.terminate((Exception) e);
			});

			thread.setDaemon(true);
			thread.start();
			LOGGER.info("Maxwell http server started on port " + port);
		}
	}

	private boolean isHttpReportingEnabled(MaxwellContext context) {
		MaxwellConfig config = context.getConfig();
		String reportingType = config.getMetricsReportingType();
		return (reportingType != null && reportingType.contains(MaxwellMetrics.reportingTypeHttp));
	}

	private MaxwellDiagnosticContext getDiagnosticContext(MaxwellContext context) {
		MaxwellDiagnosticConfig diagnosticConfig = context.getConfig().getDiagnosticConfig();
		if (diagnosticConfig.isEnable()) {
			return context.getDiagnosticContext();
		} else {
			return null;
		}
	}
}

class MaxwellHTTPServerWorker implements StoppableTask, Runnable {

	private final String bindAddress;
	private int port;
	private final String pathPrefix;
	private final MetricRegistry metricRegistry;
	private final HealthCheckRegistry healthCheckRegistry;
	private final MaxwellDiagnosticContext diagnosticContext;
	private Server server;

	public MaxwellHTTPServerWorker(String bindAddress, int port, String pathPrefix, MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, MaxwellDiagnosticContext diagnosticContext) {
		this.bindAddress = bindAddress;
		this.port = port;
		this.pathPrefix = pathPrefix;
		this.metricRegistry = metricRegistry;
		this.healthCheckRegistry = healthCheckRegistry;
		this.diagnosticContext = diagnosticContext;
	}

	public void startServer() throws Exception {
		if (this.bindAddress != null) {
			this.server = new Server(new InetSocketAddress(InetAddress.getByName(this.bindAddress), port));
		}
		else {
			this.server = new Server(this.port);
		}
		ServletContextHandler handler = new ServletContextHandler(this.server, pathPrefix);
		handler.addServlet(new ServletHolder(new MetricsServlet(metricRegistry)), "/metrics");
		handler.addServlet(new ServletHolder(new HealthCheckServlet(healthCheckRegistry)), "/healthcheck");
		handler.addServlet(new ServletHolder(new PingServlet()), "/ping");

		if (diagnosticContext != null) {
			handler.addServlet(new ServletHolder(new DiagnosticHealthCheck(diagnosticContext)), "/diagnostic");
		}

		this.server.start();
		this.server.join();
	}

	@Override
	public void run() {
		try {
			startServer();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void requestStop() throws Exception {
		this.server.stop();
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
	}
}
