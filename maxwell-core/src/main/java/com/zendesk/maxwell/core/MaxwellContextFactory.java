package com.zendesk.maxwell.core;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.ProducerInitialization;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

@Service
public class MaxwellContextFactory {
	private final MaxwellConfigFactory maxwellConfigFactory;
	private final ProducerInitialization producerInitialization;
	private final MetricRegistry metricRegistry;

	private final List<ContextStartListener> contextStartListeners;

	@Autowired
	public MaxwellContextFactory(MaxwellConfigFactory maxwellConfigFactory, ProducerInitialization producerInitialization, MetricRegistry metricRegistry, List<ContextStartListener> contextStartListeners) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.producerInitialization = producerInitialization;
		this.metricRegistry = metricRegistry;
		this.contextStartListeners = contextStartListeners;
	}

	public MaxwellSystemContext createFor(MaxwellConfig config) throws SQLException, URISyntaxException {
		MaxwellSystemContext context = new BaseMaxwellContext(config, metricRegistry, contextStartListeners);
		producerInitialization.createAndRegister(context);
		context.probeConnections();
		return context;
	}
}
