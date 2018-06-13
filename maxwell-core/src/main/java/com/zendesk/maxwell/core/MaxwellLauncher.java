package com.zendesk.maxwell.core;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MaxwellLauncher {

	private final MaxwellConfigFactory maxwellConfigFactory;
	private final MaxwellContextFactory maxwellContextFactory;
	private final Logging logging;
	private final MaxwellRunner maxwellRunner;

	@Autowired
	public MaxwellLauncher(MaxwellConfigFactory maxwellConfigFactory,
						   MaxwellContextFactory maxwellContextFactory,
						   Logging logging,
						   MaxwellRunner maxwellRunner) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.maxwellContextFactory = maxwellContextFactory;
		this.logging = logging;
		this.maxwellRunner = maxwellRunner;
	}

	public MaxwellContext launch(Properties configurationProperties) throws Exception {
		logging.configureLevelFrom(configurationProperties);
		final MaxwellConfig configuration = maxwellConfigFactory.createFor(configurationProperties);
		final MaxwellContext context = maxwellContextFactory.createFor(configuration, configurationProperties);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			maxwellRunner.terminate(context);
			StaticShutdownCallbackRegistry.invoke();
		}));

		maxwellRunner.start(context);
		return context;
	}

}
