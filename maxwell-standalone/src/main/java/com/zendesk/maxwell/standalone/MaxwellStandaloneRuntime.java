package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.api.LauncherException;
import com.zendesk.maxwell.api.MaxwellContext;
import com.zendesk.maxwell.api.MaxwellLauncher;
import com.zendesk.maxwell.metricreporter.core.MetricsReporterInitialization;
import com.zendesk.maxwell.standalone.api.MaxwellRuntime;
import com.zendesk.maxwell.standalone.api.SystemShutdownListener;
import com.zendesk.maxwell.standalone.config.ConfigurationOptionMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

@Service
public class MaxwellStandaloneRuntime implements MaxwellRuntime {
	private final static Logger LOGGER = LoggerFactory.getLogger(MaxwellStandaloneRuntime.class);

	private final ConfigurationOptionMerger configurationOptionMerger;
	private final MaxwellLauncher maxwellLauncher;
	private final MetricsReporterInitialization metricsReporterInitialization;
	private final List<SystemShutdownListener> shutdownListeners;

	private static MaxwellContext context;

	@Autowired
	public MaxwellStandaloneRuntime(ConfigurationOptionMerger configurationOptionMerger, MaxwellLauncher maxwellLauncher, MetricsReporterInitialization metricsReporterInitialization, List<SystemShutdownListener> shutdownListeners) {
		this.configurationOptionMerger = configurationOptionMerger;
		this.maxwellLauncher = maxwellLauncher;
		this.metricsReporterInitialization = metricsReporterInitialization;
		this.shutdownListeners = shutdownListeners;
	}

	public void runMaxwell(final String[] args) {
		try {
			final Properties configurationOptions = configurationOptionMerger.mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(args);
			runMaxwell(configurationOptions);
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell", e);
		}
	}

	public void runMaxwell(final Properties configurationOptions) {
		try {
			metricsReporterInitialization.setup(configurationOptions);
			context = maxwellLauncher.launch(configurationOptions);
			notifyShutdown();
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell", e);
		}
	}

	@Override
	public void shutdown() {
		getActiveContext().ifPresent(c -> context.terminate());
		notifyShutdown();
		System.exit(0);
	}

	@Override
	public void shutdown(Exception e) {
		LOGGER.error("System error occurred. Shutdown system.", e);
		getActiveContext().ifPresent(c -> context.terminate(e));
		notifyShutdown();
		System.exit(-1);
	}

	private Optional<MaxwellContext> getActiveContext() {
		return Optional.ofNullable(context);
	}

	public void notifyShutdown(){
		shutdownListeners.forEach(l -> l.onSystemShutdown());
	}
}
