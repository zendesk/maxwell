package com.zendesk.maxwell.core.producer;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.StoppableTask;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticRegistry;
import com.zendesk.maxwell.core.producer.impl.noop.NoopProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Service
public class ProducerInitialization {
	private static final String NONE_PRODUCER_TYPE = "none";

	private final ProducerConfigurators producerConfigurators;
	private final MaxwellDiagnosticRegistry maxwellDiagnosticRegistry;
	private final HealthCheckRegistry healthCheckRegistry;

	@Autowired
	public ProducerInitialization(ProducerConfigurators producerConfigurators, MaxwellDiagnosticRegistry maxwellDiagnosticRegistry, HealthCheckRegistry healthCheckRegistry) {
		this.producerConfigurators = producerConfigurators;
		this.maxwellDiagnosticRegistry = maxwellDiagnosticRegistry;
		this.healthCheckRegistry = healthCheckRegistry;
	}

	public void initialize(final MaxwellContext maxwellContext, final Properties configurationSettings){
		Producer producer = create(maxwellContext, configurationSettings);
		maxwellContext.setProducer(producer);
		registerDiagnostics(producer);
		registerHealthCheck(producer);
		registerStoppableTask(producer, maxwellContext);
	}

	private Producer create(final MaxwellContext maxwellContext, final Properties configurationSettings) {
		final MaxwellConfig config = maxwellContext.getConfig();
		if ( config.producerFactory != null ) {
			return createProducerThroughFactory(maxwellContext, config);
		} else if (config.producerType != null) {
			return createProducerThroughConfigurator(config.producerType, maxwellContext, configurationSettings);
		}
		throw new IllegalStateException("No producer configured");
	}

	private Producer createProducerThroughFactory(MaxwellContext maxwellContext, MaxwellConfig config) {
		try {
			final Class<?> clazz = Class.forName(config.producerFactory);
			final ProducerFactory producerFactory = ProducerFactory.class.cast(clazz.newInstance());
			return producerFactory.createProducer(maxwellContext);
		} catch (ClassNotFoundException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class "+config.producerFactory+" not found", e, "--custom_producer.factory");
		} catch (IllegalAccessException | InstantiationException | ClassCastException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class instantiation error", e, "--custom_producer.factory");
		}
	}

	public Producer createProducerThroughConfigurator(String type, MaxwellContext maxwellContext, Properties configurationSettings){
		if(NONE_PRODUCER_TYPE.equals(type)){
			return new NoopProducer(maxwellContext);
		}
		ProducerConfigurator configurator = producerConfigurators.getByIdentifier(type);
		ProducerConfiguration configuration = configurator.parseConfiguration(configurationSettings).orElseGet(PropertiesProducerConfiguration::new);
		configuration.validate();
		return configurator.configure(maxwellContext, configuration);
	}

	private void registerDiagnostics(Producer producer) {
		if (producer != null && producer.getDiagnostic() != null) {
			maxwellDiagnosticRegistry.registerDiagnostic(producer.getDiagnostic());
		}
	}

	private void registerHealthCheck(Producer producer) {
		if (producer != null && producer.getDiagnostic() != null) {
			healthCheckRegistry.register("MaxwellHealth.Producer", new ProducerHealthCheck(producer));
		}
	}

	private void registerStoppableTask(Producer producer, MaxwellContext maxwellContext) {
		StoppableTask task = null;
		if (producer != null) {
			task = producer.getStoppableTask();
		}
		if (task != null) {
			maxwellContext.addTask(task);
		}
	}

}
