package com.zendesk.maxwell.metricreporter.http;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnosticRegistry;
import com.zendesk.maxwell.api.monitoring.MetricReporter;
import com.zendesk.maxwell.standalone.api.MaxwellRuntime;
import com.zendesk.maxwell.standalone.api.SystemShutdownListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class HttpMetricReporter implements MetricReporter<HttpMetricReporterConfiguration>, SystemShutdownListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpMetricReporter.class);

    private final MetricRegistry metricRegistry;
    private final HealthCheckRegistry healthCheckRegistry;
    private final MaxwellDiagnosticRegistry diagnosticRegistry;
    private final MaxwellRuntime maxwellRuntime;

    private static HttpMetricServerWorker serverWorker;

    public HttpMetricReporter(MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry, MaxwellDiagnosticRegistry diagnosticRegistry, MaxwellRuntime maxwellRuntime) {
        this.metricRegistry = metricRegistry;
        this.healthCheckRegistry = healthCheckRegistry;
        this.diagnosticRegistry = diagnosticRegistry;
        this.maxwellRuntime = maxwellRuntime;
    }

    @Override
    public void start(HttpMetricReporterConfiguration configuration) {
        LOGGER.info("Maxwell http server starting");
        int port = configuration.getHttpPort();

        serverWorker = new HttpMetricServerWorker(configuration, metricRegistry, healthCheckRegistry, diagnosticRegistry);
        Thread thread = new Thread(serverWorker);

        thread.setUncaughtExceptionHandler((t, e) -> {
            LOGGER.error("Maxwell http server failure", e);
            maxwellRuntime.shutdown((Exception) e);
        });

        thread.setDaemon(true);
        thread.start();
        LOGGER.info("Maxwell http server started on port " + port);
    }

    @Override
    public void onSystemShutdown() {
        if(serverWorker != null){
            serverWorker.stop();
        }
    }
}
