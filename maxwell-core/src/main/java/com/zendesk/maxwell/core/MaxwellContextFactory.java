package com.zendesk.maxwell.core;

import com.codahale.metrics.MetricRegistry;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.Producers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

@Service
public class MaxwellContextFactory {
	private final MaxwellConfigFactory maxwellConfigFactory;
	private final Producers producers;
	private final MetricRegistry metricRegistry;

	private final List<ContextStartListener> contextStartListeners;

	@Autowired
	public MaxwellContextFactory(MaxwellConfigFactory maxwellConfigFactory, Producers producers, MetricRegistry metricRegistry, List<ContextStartListener> contextStartListeners) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.producers = producers;
		this.metricRegistry = metricRegistry;
		this.contextStartListeners = contextStartListeners;
	}

	public MaxwellContext createFor(Properties configurationOptions) throws SQLException, URISyntaxException {
		return createFor(configurationOptions, (c) -> {});
	}

	public MaxwellSystemContext createFor(Properties configurationOptions, Consumer<MaxwellConfig> configurationAdopter) throws SQLException, URISyntaxException {
		final MaxwellConfig config = maxwellConfigFactory.createFor(configurationOptions);
		configurationAdopter.accept(config);
		return createFor(config, configurationOptions);
	}

	public MaxwellSystemContext createFor(MaxwellConfig config) throws SQLException, URISyntaxException {
		return createFor(config, new Properties());
	}

	private MaxwellSystemContext createFor(MaxwellConfig config, Properties configurationOptions) throws SQLException, URISyntaxException {
		MaxwellSystemContext context = new BaseMaxwellContext(config, metricRegistry, contextStartListeners);
		producers.createAndRegister(context, configurationOptions);
		context.probeConnections();
		return context;
	}
}
