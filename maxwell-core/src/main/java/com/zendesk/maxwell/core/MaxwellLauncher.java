package com.zendesk.maxwell.core;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import com.zendesk.maxwell.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.function.Consumer;

@Service
public class MaxwellLauncher {

	private final MaxwellConfigFactory maxwellConfigFactory;
	private final MaxwellContextFactory maxwellContextFactory;
	private final ConfigurationSupport configurationSupport;
	private final Logging logging;
	private final MaxwellRunner maxwellRunner;

	@Autowired
	public MaxwellLauncher(MaxwellConfigFactory maxwellConfigFactory, MaxwellContextFactory maxwellContextFactory, ConfigurationSupport configurationSupport, Logging logging, MaxwellRunner maxwellRunner) {
		this.maxwellConfigFactory = maxwellConfigFactory;
		this.maxwellContextFactory = maxwellContextFactory;
		this.configurationSupport = configurationSupport;
		this.logging = logging;
		this.maxwellRunner = maxwellRunner;
	}

	public void launch(Properties configurationProperties, Consumer<MaxwellConfig> configurationAdopter) throws Exception {
		final MaxwellConfig configuration = maxwellConfigFactory.createFor(configurationProperties);
		configurationAdopter.accept(configuration);
		final MaxwellSystemContext context = maxwellContextFactory.createFor(configuration);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			maxwellRunner.terminate(context);
			StaticShutdownCallbackRegistry.invoke();
		}));

		maxwellRunner.start(context);
	}

	private void setupLogging(final Properties configurationOptions){
		String logLevel = configurationSupport.fetchOption("log_level", configurationOptions, null);
		if(logLevel != null){
			logging.setLevel(logLevel);
		}
	}


}
