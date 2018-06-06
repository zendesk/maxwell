package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.core.util.StoppableTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Service
public class Producers {

	private static final String NONE_PRODUCER_TYPE = "none";
	private final List<ProducerConfigurator> producerConfigurator;

	@Autowired
	public Producers(List<ProducerConfigurator> producerConfigurator) {
		this.producerConfigurator = producerConfigurator;
	}

	public void createAndRegister(final MaxwellSystemContext maxwellContext, final Properties settings){
		final MaxwellConfig config = maxwellContext.getConfig();
		ProducerContext producerContext = create(maxwellContext, settings, config);
		register(maxwellContext, producerContext);
	}

	private ProducerContext create(MaxwellContext maxwellContext, Properties settings, MaxwellConfig config) {
		if ( config.getCustomProducerFactory() != null ) {
			return createContextForCustomProducer(maxwellContext, config);
		} else {
			return createContextForPredefinedProducer(maxwellContext, settings);
		}
	}

	private ProducerContext createContextForCustomProducer(MaxwellContext maxwellContext, MaxwellConfig config) {
		PropertiesProducerConfiguration configuration = new PropertiesProducerConfiguration(config.getCustomProducerProperties());
		ProducerFactory producerFactory = initializeCustomProducerFactory(config);
		Producer producer = producerFactory.createProducer(maxwellContext);
		return new ProducerContext(configuration, producer);
	}

	private ProducerFactory initializeCustomProducerFactory(final MaxwellConfig config) {
		try {
			Class<?> clazz = Class.forName(config.getCustomProducerFactory());
			return ProducerFactory.class.cast(clazz.newInstance());
		} catch (ClassNotFoundException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class "+config.getCustomProducerFactory()+" not found", e, "--custom_producer.factory");
		} catch (IllegalAccessException | InstantiationException | ClassCastException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class instantiation error", e, "--custom_producer.factory");
		}
	}

	private ProducerContext createContextForPredefinedProducer(MaxwellContext maxwellContext, Properties settings) {
		String producerType = maxwellContext.getConfig().getProducerType();
		if(producerType == null || NONE_PRODUCER_TYPE.equals(producerType)){
			return new ProducerContext(new PropertiesProducerConfiguration(new Properties()), new NoopProducer(maxwellContext));
		}

		ProducerConfigurator configurator = getByIdentifier(producerType);
		return configurator.createProducerContext(maxwellContext, settings);
	}

	private ProducerConfigurator getByIdentifier(final String identifier){
		return producerConfigurator.stream().filter(e -> isProducerWithIdentifier(identifier, e)).findFirst().orElseThrow(() -> new RuntimeException("Unknown producer identifier: " + identifier));
	}

	private void register(MaxwellSystemContext maxwellContext, ProducerContext producerContext) {
		maxwellContext.setProducerContext(producerContext);

		registerDiagnostics(producerContext.getProducer(), maxwellContext);
		registerStoppableTask(producerContext.getProducer(), maxwellContext);

	}

	private boolean isProducerWithIdentifier(String identifier, ProducerConfigurator e) {
		return e.getExtensionIdentifier().equals(identifier);
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
