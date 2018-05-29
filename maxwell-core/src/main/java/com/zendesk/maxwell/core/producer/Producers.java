package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.util.StoppableTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Service
public class Producers {

	private static final String NONE_PRODUCER_TYPE = "none";
	private final List<NamedProducerFactory> producerFactories;

	@Autowired
	public Producers(List<NamedProducerFactory> producerFactories) {
		this.producerFactories = producerFactories;
	}

	public Producer getProducer(MaxwellContext maxwellContext){
		return maxwellContext.getProducer().orElseGet(() -> createAndRegisterProducer(maxwellContext));
	}

	private Producer createAndRegisterProducer(MaxwellContext maxwellContext) {
		Producer producer = createProducer(maxwellContext);
		maxwellContext.setProducer(producer);
		registerDiagnostics(producer, maxwellContext);
		registerStoppableTask(producer, maxwellContext);
		return producer;
	}

	private Producer createProducer(MaxwellContext maxwellContext){
		MaxwellConfig config = maxwellContext.getConfig();
		if ( config.getProducerFactory() != null ) {
			return config.getProducerFactory().createProducer(maxwellContext);
		} else {
			return createProducerForType(maxwellContext);
		}
	}

	private Producer createProducerForType(MaxwellContext context){
		String producerType = context.getConfig().getProducerType();
		return NONE_PRODUCER_TYPE.equals(producerType) ? null : createProducerFactory(producerType).createProducer(context);
	}

	private ProducerFactory createProducerFactory(String type){
		return producerFactories.stream().filter(pf -> pf.getName().equals(type)).findFirst().orElseThrow(() -> new RuntimeException("Unknown producer type: " + type));
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
