package com.zendesk.maxwell.core.monitoring;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.core.ContextStartListener;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.producer.ProducerExtensionConfigurators;
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

	private final ProducerExtensionConfigurators producerExtensionConfigurators;

	@Autowired
	public MaxwellHTTPServer(ProducerExtensionConfigurators producerExtensionConfigurators) {
		this.producerExtensionConfigurators = producerExtensionConfigurators;
	}

	@Override
	public void onContextStart(MaxwellContext context) {
		startIfRequired(context);
	}

	private void startIfRequired(MaxwellContext context) {
		MaxwellMetrics.Registries metricsRegistries = getMetricsRegistries(context);
		MaxwellDiagnosticContext diagnosticContext = getDiagnosticContext(context);
		if (metricsRegistries != null || diagnosticContext != null) {
			LOGGER.info("Maxwell http server starting");
			int port = context.getConfig().getHttpPort();
			String httpBindAddress = context.getConfig().getHttpBindAddress();
			String pathPrefix = context.getConfig().getHttpPathPrefix();
			MaxwellHTTPServerWorker maxwellHTTPServerWorker = new MaxwellHTTPServerWorker(httpBindAddress, port, pathPrefix, metricsRegistries, diagnosticContext);
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

	private MaxwellMetrics.Registries getMetricsRegistries(MaxwellContext context) {
		MaxwellConfig config = context.getConfig();
		String reportingType = config.getMetricsReportingType();
		if (reportingType != null && reportingType.contains(MaxwellMetrics.reportingTypeHttp)) {
			config.getHealthCheckRegistry().register("MaxwellHealth", new MaxwellHealthCheck(producerExtensionConfigurators.getProducer(context)));
			return new MaxwellMetrics.Registries(config.getMetricRegistry(), config.getHealthCheckRegistry());
		} else {
			return null;
		}
	}

	private MaxwellDiagnosticContext getDiagnosticContext(MaxwellContext context) {
		MaxwellDiagnosticContext.Config diagnosticConfig = context.getConfig().getDiagnosticConfig();
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
	private Server server;

	public MaxwellHTTPServerWorker(String bindAddress, int port, String pathPrefix, MaxwellMetrics.Registries metricsRegistries, MaxwellDiagnosticContext diagnosticContext) {
		this.bindAddress = bindAddress;
		this.port = port;
		this.pathPrefix = pathPrefix;
		this.metricsRegistries = metricsRegistries;
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

		if (metricsRegistries != null) {
			// TODO: there is a way to wire these up automagically via the AdminServlet, but it escapes me right now
			handler.addServlet(new ServletHolder(new MetricsServlet(metricsRegistries.metricRegistry)), "/metrics");
			handler.addServlet(new ServletHolder(new HealthCheckServlet(metricsRegistries.healthCheckRegistry)), "/healthcheck");
			handler.addServlet(new ServletHolder(new PingServlet()), "/ping");
		}

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
