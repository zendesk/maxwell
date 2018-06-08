package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.LauncherException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.bootstrap.MaxwellBootstrapUtilityLauncher;
import com.zendesk.maxwell.core.bootstrap.config.MaxwellBootstrapUtilConfigurationOptionMerger;
import com.zendesk.maxwell.core.config.ConfigurationOptionMerger;
import com.zendesk.maxwell.core.config.MaxwellConfigurationOptionMerger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;
import java.util.function.Consumer;

public class SpringLauncher {

	public static void launchMaxwell(final String[] args){
		launch((applicationContext -> runMaxwell(args, (c) -> {}, applicationContext)));
	}

	public static void runMaxwell(final String[] args, Consumer<MaxwellConfig> configurationAdopter, final ApplicationContext applicationContext) {
		try {
			final ConfigurationOptionMerger configurationOptionMerger = applicationContext.getBean(MaxwellConfigurationOptionMerger.class);
			final MaxwellLauncher maxwellLauncher = applicationContext.getBean(MaxwellLauncher.class);
			final Properties configurationOptions = configurationOptionMerger.mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(args);

			maxwellLauncher.launch(configurationOptions, configurationAdopter);
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell", e);
		}
	}

	public static void launchBootstrapperUtility(final String[] args){
		launch(applicationContext -> runBootstrapperUtility(args, applicationContext));
	}

	private static void runBootstrapperUtility(final String[] args, final ApplicationContext applicationContext){
		try {
			final ConfigurationOptionMerger configurationOptionMerger = applicationContext.getBean(MaxwellBootstrapUtilConfigurationOptionMerger.class);
			final MaxwellBootstrapUtilityLauncher maxwellBootstrapUtilityLauncher = applicationContext.getBean(MaxwellBootstrapUtilityLauncher.class);
			final Properties configurationOptions = configurationOptionMerger.mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(args);
			maxwellBootstrapUtilityLauncher.launch(configurationOptions);
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell Bootstrapper Utility", e);
		}
	}

	public static void launch(Consumer<ApplicationContext> applicationContextConsumer){
		try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
			ctx.register(SpringLauncherScanConfig.class);
			ctx.refresh();

			applicationContextConsumer.accept(ctx);
		}
	}
}
