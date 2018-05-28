package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.util.StoppableTask;
import com.zendesk.maxwell.core.util.TaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Service
public class Producers {

	private static final String NONE_PRODUCER_TYPE = "none";
	private final TaskManager taskManager;
	private final List<NamedProducerFactory> producerFactories;

	private Producer producer;

	@Autowired
	public Producers(TaskManager taskManager, List<NamedProducerFactory> producerFactories) {
		this.taskManager = taskManager;
		this.producerFactories = producerFactories;
	}

	public Producer getProducer(MaxwellContext maxwellContext){
		return this.producer != null ? this.producer : createAndRegisterProducer(maxwellContext);
	}

	private Producer createAndRegisterProducer(MaxwellContext maxwellContext) {
		MaxwellConfig config = maxwellContext.getConfig();
		if ( config.producerFactory != null ) {
			this.producer = config.producerFactory.createProducer(maxwellContext);
		} else {
			this.producer = createProducer(maxwellContext);
		}

		registerDiagnostics(maxwellContext);
		registerStoppableTask();
		return this.producer;
	}

	private Producer createProducer(MaxwellContext context){
		String producerType = context.getConfig().producerType;
		return NONE_PRODUCER_TYPE.equals(producerType) ? null : createProducerFactory(producerType).createProducer(context);
	}

	private ProducerFactory createProducerFactory(String type){
		return producerFactories.stream().filter(pf -> pf.getName().equals(type)).findFirst().orElseThrow(() -> new RuntimeException("Unknown producer type: " + type));
	}

	private void registerDiagnostics(MaxwellContext maxwellContext) {
		if (this.producer != null && this.producer.getDiagnostic() != null) {
			maxwellContext.getDiagnosticContext().diagnostics.add(producer.getDiagnostic());
		}
	}

	private void registerStoppableTask() {
		StoppableTask task = null;
		if (producer != null) {
			task = producer.getStoppableTask();
		}
		if (task != null) {
			taskManager.add(task);
		}
	}

}
