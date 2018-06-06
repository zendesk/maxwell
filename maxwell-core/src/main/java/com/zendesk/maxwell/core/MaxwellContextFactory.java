package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.producer.ProducerConfigurators;
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
	private final ProducerConfigurators producerConfigurators;

	private final List<ContextStartListener> contextStartListeners;

	@Autowired
	public MaxwellContextFactory(MaxwellConfigFactory maxwellConfigFactory, ProducerConfigurators producerConfigurators, List<ContextStartListener> contextStartListeners) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.producerConfigurators = producerConfigurators;
		this.contextStartListeners = contextStartListeners;
	}

	public MaxwellContext createFor(Properties configurationOptions) throws SQLException, URISyntaxException {
		return createFor(configurationOptions, (c) -> {});
	}

	public MaxwellContext createFor(Properties configurationOptions, Consumer<MaxwellConfig> configurationAdopter) throws SQLException, URISyntaxException {
		final MaxwellConfig config = maxwellConfigFactory.createFor(configurationOptions);
		configurationAdopter.accept(config);
		return createFor(config, configurationOptions);
	}

	public MaxwellContext createFor(MaxwellConfig config) throws SQLException, URISyntaxException {
		return createFor(config, new Properties());
	}

	private MaxwellContext createFor(MaxwellConfig config, Properties configurationOptions) throws SQLException, URISyntaxException {
		MaxwellContext context = new BaseMaxwellContext(config, contextStartListeners);
		producerConfigurators.createAndRegister(context, configurationOptions);
		context.probeConnections();
		return context;
	}
}
