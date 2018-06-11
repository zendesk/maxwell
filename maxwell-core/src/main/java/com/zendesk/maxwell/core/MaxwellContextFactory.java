package com.zendesk.maxwell.core;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.monitoring.MaxwellDiagnosticRegistry;
import com.zendesk.maxwell.core.producer.ProducerInitialization;
import com.zendesk.maxwell.core.replication.BinlogConnectorDiagnostic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

@Service
public class MaxwellContextFactory {
	private final ProducerInitialization producerInitialization;
	private final MetricRegistry metricRegistry;
	private final MaxwellDiagnosticRegistry maxwellDiagnosticRegistry;

	@Autowired
	public MaxwellContextFactory(ProducerInitialization producerInitialization, MetricRegistry metricRegistry, MaxwellDiagnosticRegistry maxwellDiagnosticRegistry) {
		this.producerInitialization = producerInitialization;
		this.metricRegistry = metricRegistry;
		this.maxwellDiagnosticRegistry = maxwellDiagnosticRegistry;
	}

	public MaxwellSystemContext createFor(MaxwellConfig config) throws SQLException, URISyntaxException {
		return createFor(config, new Properties());
	}

	public MaxwellSystemContext createFor(MaxwellConfig config, Properties configurationProperties) throws SQLException, URISyntaxException {
		MaxwellSystemContext context = new BaseMaxwellContext(config, metricRegistry);
		producerInitialization.initialize(context, configurationProperties);
		maxwellDiagnosticRegistry.registerDiagnostic(new BinlogConnectorDiagnostic(context));
		context.probeConnections();
		return context;
	}
}
