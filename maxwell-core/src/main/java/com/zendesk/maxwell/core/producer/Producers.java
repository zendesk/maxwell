package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.ExtensionConfigurator;
import com.zendesk.maxwell.core.config.ExtensionType;
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
	private final List<ExtensionConfigurator<Producer>> extensionConfigurator;

	@Autowired
	public Producers(List<ExtensionConfigurator<Producer>> extensionConfigurator) {
		this.extensionConfigurator = extensionConfigurator;
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
		return NONE_PRODUCER_TYPE.equals(producerType) ? null : createProducerFromConfigurator(context, producerType);
	}

	private Producer createProducerFromConfigurator(MaxwellContext context, String identifier){
		return extensionConfigurator.stream()
				.filter(e -> isProducerWithIdentifier(identifier, e))
				.findFirst()
				.map(e -> e.createInstance(context))
				.orElseThrow(() -> new RuntimeException("Unknown producer identifier: " + identifier));
	}

	private boolean isProducerWithIdentifier(String type, ExtensionConfigurator<Producer> e) {
		return e.getExtensionType() == ExtensionType.PROVIDER && e.getExtensionIdentifier().equals(type);
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
