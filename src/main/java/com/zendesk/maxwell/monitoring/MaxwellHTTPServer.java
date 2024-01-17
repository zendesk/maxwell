package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.util.StoppableTask;
import jnr.ffi.annotations.In;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static com.zendesk.maxwell.monitoring.MaxwellMetrics.reportingTypeHttp;

public class MaxwellHTTPServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHTTPServer.class);

	public static void startIfRequired(MaxwellContext context) throws IOException {
		MaxwellMetrics.Registries metricsRegistries = getMetricsRegistries(context);
		MaxwellDiagnosticContext diagnosticContext = getDiagnosticContext(context);
		if (metricsRegistries != null || diagnosticContext != null || context.getConfig().enableHttpConfig) {
			LOGGER.info("Maxwell http server starting");
			int port = context.getConfig().httpPort;
			String httpBindAddress = context.getConfig().httpBindAddress;
			String pathPrefix = context.getConfig().httpPathPrefix;
			MaxwellHTTPServerWorker maxwellHTTPServerWorker = new MaxwellHTTPServerWorker(httpBindAddress, port, pathPrefix, metricsRegistries, diagnosticContext, context);
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

	private static MaxwellMetrics.Registries getMetricsRegistries(MaxwellContext context) throws IOException {
		MaxwellConfig config = context.getConfig();
		String reportingType = config.metricsReportingType;
		if (reportingType != null && reportingType.contains(reportingTypeHttp)) {
			if (config.customHealthFactory != null) {
				context.healthCheckRegistry.register("MaxwellHealth", config.customHealthFactory.createHealthCheck(context.getProducer()));
			} else {
				context.healthCheckRegistry.register("MaxwellHealth", new MaxwellHealthCheck(context.getProducer()));
			}
			return new MaxwellMetrics.Registries(context.metricRegistry, context.healthCheckRegistry);
		} else {
			return null;
		}
	}

	private static MaxwellDiagnosticContext getDiagnosticContext(MaxwellContext context) {
		MaxwellDiagnosticContext.Config diagnosticConfig = context.getConfig().diagnosticConfig;
		if (diagnosticConfig.enable) {
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
	private final MaxwellMetrics.Registries metricsRegistries;
	private final MaxwellDiagnosticContext diagnosticContext;
	private final MaxwellContext context;
	private Server server;

	public MaxwellHTTPServerWorker(String bindAddress, int port, String pathPrefix, MaxwellMetrics.Registries metricsRegistries, MaxwellDiagnosticContext diagnosticContext, MaxwellContext context) {
		this.bindAddress = bindAddress;
		this.port = port;
		this.pathPrefix = pathPrefix;
		this.metricsRegistries = metricsRegistries;
		this.diagnosticContext = diagnosticContext;
		this.context = context;
	}

	public void startServer() throws Exception {
		IndexListServlet indexList = new IndexListServlet();

		if (this.bindAddress != null) {
			this.server = new Server(new InetSocketAddress(InetAddress.getByName(this.bindAddress), port));
		}
		else {
			this.server = new Server(this.port);
		}
		ServletContextHandler handler = new ServletContextHandler(this.server, pathPrefix);

		handler.addServlet(new ServletHolder(indexList), "/");

		if (metricsRegistries != null) {
			// TODO: there is a way to wire these up automagically via the AdminServlet, but it escapes me right now
			handler.addServlet(new ServletHolder(new MetricsServlet(metricsRegistries.metricRegistry)), "/metrics");
			handler.addServlet(new ServletHolder(new io.prometheus.client.exporter.MetricsServlet()), "/prometheus");
			handler.addServlet(new ServletHolder(new HealthCheckServlet(metricsRegistries.healthCheckRegistry)), "/healthcheck");
			handler.addServlet(new ServletHolder(new PingServlet()), "/ping");

			indexList.addLink("/metrics", "codahale metrics");
			indexList.addLink("/prometheus", "prometheus metrics");
			indexList.addLink("/healthcheck", "healthcheck endpoint");
			indexList.addLink("/ping", "ping me");
		}

		if (this.context.getConfig().enableHttpConfig) {
			handler.addServlet(new ServletHolder(new MaxwellConfigServlet(this.context)), "/config");
			indexList.addLink("/config", "POST endpoing to update maxwell config.");
		}

		if (diagnosticContext != null) {
			handler.addServlet(new ServletHolder(new DiagnosticHealthCheck(diagnosticContext)), "/diagnostic");
			indexList.addLink("/diagnostic", "deeper diagnostic health checks");
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
