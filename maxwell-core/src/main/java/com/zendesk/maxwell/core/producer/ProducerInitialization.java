package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.StoppableTask;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.producer.*;
import com.zendesk.maxwell.core.MaxwellSystemContext;
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

	@Autowired
	public ProducerInitialization(ProducerConfigurators producerConfigurators) {
		this.producerConfigurators = producerConfigurators;
	}

	public void initialize(final MaxwellSystemContext maxwellContext, final Properties configurationSettings){
		Producer producer = create(maxwellContext, configurationSettings);
		maxwellContext.setProducer(producer);
		registerDiagnostics(producer, maxwellContext);
		registerStoppableTask(producer, maxwellContext);
	}

	private Producer create(final MaxwellContext maxwellContext, final Properties configurationSettings) {
		final MaxwellConfig config = maxwellContext.getConfig();
		if ( config.getProducerFactory() != null ) {
			return createProducerThroughFactory(maxwellContext, config);
		} else if (config.getProducerType() != null) {
			return createProducerThroughConfigurator(config.getProducerType(), maxwellContext, configurationSettings);
		}
		throw new IllegalStateException("No producer configured");
	}

	private Producer createProducerThroughFactory(MaxwellContext maxwellContext, MaxwellConfig config) {
		try {
			final Class<?> clazz = Class.forName(config.getProducerFactory());
			final ProducerFactory producerFactory = ProducerFactory.class.cast(clazz.newInstance());
			return producerFactory.createProducer(maxwellContext);
		} catch (ClassNotFoundException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class "+config.getProducerFactory()+" not found", e, "--custom_producer.factory");
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
		return configurator.configure(maxwellContext, configuration);
	}

	private void registerDiagnostics(Producer producer, MaxwellContext maxwellContext) {
		if (producer != null && producer.getDiagnostic() != null) {
			maxwellContext.getDiagnosticContext().diagnostics.add(producer.getDiagnostic());
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
