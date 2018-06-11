package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.api.LauncherException;
import com.zendesk.maxwell.api.bootstrap.MaxwellBootstrapUtilityLauncher;
import com.zendesk.maxwell.standalone.config.MaxwellBootstrapUtilConfigurationOptionMerger;
import com.zendesk.maxwell.standalone.config.ConfigurationOptionMerger;
import com.zendesk.maxwell.standalone.springconfig.StandaloneApplicationComponentScanConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Properties;
import java.util.function.Consumer;

public class SpringLauncher {

	public static void launchMaxwell(final String[] args){
		launch(withMaxWellSpringLauncher( m -> m.runMaxwell(args)));
	}

	private static Consumer<ApplicationContext> withMaxWellSpringLauncher(Consumer<MaxwellStandaloneRuntime> consumer){
		return (ctx) -> {
			MaxwellStandaloneRuntime launcher = ctx.getBean(MaxwellStandaloneRuntime.class);
			consumer.accept(launcher);
		};
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
			ctx.register(StandaloneApplicationComponentScanConfig.class);
			ctx.refresh();

			applicationContextConsumer.accept(ctx);
		}
	}
}
