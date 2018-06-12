package com.zendesk.maxwell.metricreporter.core.jmx;

import com.zendesk.maxwell.metricreporter.core.MetricReporterConfiguration;
import com.zendesk.maxwell.metricreporter.core.MetricReporterConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JmxMetricReporterConfigurator implements MetricReporterConfigurator {
    private final JmxMetricReporter reporter;

    @Autowired
    public JmxMetricReporterConfigurator(JmxMetricReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public String getIdentifier() {
        return "jmx";
    }

    @Override
    public void enableReporter(MetricReporterConfiguration configuration) {
        reporter.start(configuration);
    }
}
