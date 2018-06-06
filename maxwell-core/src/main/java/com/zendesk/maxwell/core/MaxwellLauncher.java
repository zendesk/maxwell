package com.zendesk.maxwell.core;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.core.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.function.Consumer;

@Service
public class MaxwellLauncher {

	private final ConfigurationSupport configurationSupport;
	private final MaxwellContextFactory maxwellContextFactory;
	private final MaxwellRunner maxwellRunner;
	private final Logging logging;

	@Autowired
	public MaxwellLauncher(ConfigurationSupport configurationSupport, MaxwellContextFactory maxwellContextFactory, MaxwellRunner maxwellRunner, Logging logging) {
		this.configurationSupport = configurationSupport;
		this.maxwellContextFactory = maxwellContextFactory;
		this.maxwellRunner = maxwellRunner;
		this.logging = logging;
	}

	public void launch(final Properties configurationOptions, Consumer<MaxwellConfig> configurationAdopter) throws Exception {
		setupLogging(configurationOptions);
		final MaxwellContext context = maxwellContextFactory.createFor(configurationOptions, configurationAdopter);

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
