package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.StoppableTask;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.producer.*;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Service
public class ProducerInitialization {

	private final ApplicationContext applicationContext;

	@Autowired
	public ProducerInitialization(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	public void createAndRegister(final MaxwellSystemContext maxwellContext){
		final MaxwellConfig config = maxwellContext.getConfig();
		Producer producer = create(maxwellContext, config);
		register(maxwellContext, producer);
	}

	private Producer create(MaxwellContext maxwellContext, MaxwellConfig config) {
		if ( config.getProducerFactory() != null ) {
			return createProducerThroughFactory(maxwellContext, config);
		} else {
			throw new IllegalStateException("No producer configured");
		}
	}

	private Producer createProducerThroughFactory(MaxwellContext maxwellContext, MaxwellConfig config) {
		final Class<? extends ProducerFactory> clazz = getProducerFactoryClass(config);
		final ProducerFactory producerFactory = getProducerFactoryFromSpringContext(clazz).orElseGet(() -> initializeProducerFactoryByClass(clazz));
		return producerFactory.createProducer(maxwellContext);
	}

	private Optional<ProducerFactory> getProducerFactoryFromSpringContext(Class<? extends ProducerFactory> clazz) {
		try {
			return Optional.of(applicationContext.getBean(clazz));
		}catch (BeansException e){
			return Optional.empty();
		}
	}

	private ProducerFactory initializeProducerFactoryByClass(Class<? extends ProducerFactory> clazz) {
		try {
			return clazz.newInstance();
		} catch (IllegalAccessException | InstantiationException | ClassCastException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory, class instantiation error", e, "--custom_producer.factory");
		}
	}

	private Class<? extends ProducerFactory> getProducerFactoryClass(MaxwellConfig config) {
		try {
			return (Class<? extends ProducerFactory>)Class.forName(config.getProducerFactory());
		} catch (ClassNotFoundException e) {
			throw new InvalidOptionException("Invalid value for custom_producer.factory or invalid producer_type, class "+config.getProducerFactory()+" not found", e, "--custom_producer.factory", "--producer_type");
		}
	}

	private void register(MaxwellSystemContext maxwellContext, Producer producer) {
		maxwellContext.setProducer(producer);

		registerDiagnostics(producer, maxwellContext);
		registerStoppableTask(producer, maxwellContext);

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
