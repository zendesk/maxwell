package com.zendesk.maxwell.core.bootstrap;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.core.bootstrap.config.MaxwellBootstrapUtilityConfig;
import com.zendesk.maxwell.core.bootstrap.config.MaxwellBootstrapUtilityConfigFactory;
import com.zendesk.maxwell.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MaxwellBootstrapUtilityLauncher {

	private final MaxwellBootstrapUtilityConfigFactory maxwellBootstrapUtilityConfigFactory;
	private final ConfigurationSupport configurationSupport;
	private final Logging logging;
	private final MaxwellBootstrapUtilityRunner maxwellBootstrapUtilityRunner;

	@Autowired
	public MaxwellBootstrapUtilityLauncher(MaxwellBootstrapUtilityConfigFactory maxwellBootstrapUtilityConfigFactory,
										   ConfigurationSupport configurationSupport,
										   Logging logging,
										   MaxwellBootstrapUtilityRunner maxwellBootstrapUtilityRunner) {
		this.maxwellBootstrapUtilityConfigFactory = maxwellBootstrapUtilityConfigFactory;
		this.configurationSupport = configurationSupport;
		this.logging = logging;
		this.maxwellBootstrapUtilityRunner = maxwellBootstrapUtilityRunner;
	}

	public void launch(Properties configurationProperties) throws Exception {
		setupLogging(configurationProperties);
		final MaxwellBootstrapUtilityConfig configuration = maxwellBootstrapUtilityConfigFactory.createFor(configurationProperties);
		maxwellBootstrapUtilityRunner.run(configuration);
	}

	private void setupLogging(final Properties configurationOptions){
		String logLevel = configurationSupport.fetchOption("log_level", configurationOptions, null);
		if(logLevel != null){
			logging.setLevel(logLevel);
		}
	}


}
