package com.zendesk.maxwell.core;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MaxwellLauncher {

	private final MaxwellContextFactory maxwellContextFactory;
	private final MaxwellRunner maxwellRunner;

	@Autowired
	public MaxwellLauncher(MaxwellContextFactory maxwellContextFactory, MaxwellRunner maxwellRunner) {
		this.maxwellContextFactory = maxwellContextFactory;
		this.maxwellRunner = maxwellRunner;
	}

	public void launch(MaxwellConfig configuration) throws Exception {
		final MaxwellSystemContext context = maxwellContextFactory.createFor(configuration);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			maxwellRunner.terminate(context);
			StaticShutdownCallbackRegistry.invoke();
		}));

		maxwellRunner.start(context);
	}


}
