package com.zendesk.maxwell.metricreporter.http;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticRegistry;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class HttpMetricServerWorker implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpMetricServerWorker.class);

    private final HttpMetricReporterConfiguration configuration;
    private final MetricRegistry metricRegistry;
    private final HealthCheckRegistry healthCheckRegistry;
    private final MaxwellDiagnosticRegistry diagnosticRegistry;
    private Server server;

    public HttpMetricServerWorker(HttpMetricReporterConfiguration configuration, MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, MaxwellDiagnosticRegistry diagnosticRegistry) {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        this.healthCheckRegistry = healthCheckRegistry;
        this.diagnosticRegistry = diagnosticRegistry;
    }

    public void startServer() throws Exception {
        if (this.configuration.getHttpBindAddress() != null) {
            this.server = new Server(new InetSocketAddress(InetAddress.getByName(configuration.getHttpBindAddress()), configuration.getHttpPort()));
        }
        else {
            this.server = new Server(configuration.getHttpPort());
        }
        ServletContextHandler handler = new ServletContextHandler(this.server, configuration.getHttpPathPrefix());
        handler.addServlet(new ServletHolder(new MetricsServlet(metricRegistry)), "/metrics");
        handler.addServlet(new ServletHolder(new HealthCheckServlet(healthCheckRegistry)), "/healthcheck");
        handler.addServlet(new ServletHolder(new PingServlet()), "/ping");

        if (diagnosticRegistry != null) {
            handler.addServlet(new ServletHolder(new DiagnosticHealthCheck(configuration, diagnosticRegistry)), "/diagnostic");
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

    public void stop() {
        try {
            this.server.stop();
        } catch (Exception e) {
            LOGGER.error("Failed to stop HTTP server");
        }
    }

}
