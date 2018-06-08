package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.LauncherException;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.core.bootstrap.MaxwellBootstrapUtilityRunner;
import com.zendesk.maxwell.core.config.MaxwellConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.function.Consumer;

public class SpringLauncher {

	public static void launchMaxwell(final String[] args){
		launch((applicationContext -> runMaxwell(args, (c) -> {}, applicationContext)));
	}

	public static void runMaxwell(final String[] args, Consumer<MaxwellConfig> configurationAdopter, final ApplicationContext applicationContext) {
		try {
			final MaxwellConfigurator maxwellConfigurator = applicationContext.getBean(MaxwellConfigurator.class);
			final MaxwellLauncher maxwellLauncher = applicationContext.getBean(MaxwellLauncher.class);

			final MaxwellConfig config = maxwellConfigurator.prepareMaxwell(args);
			configurationAdopter.accept(config);

			maxwellLauncher.launch(config);
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell", e);
		}
	}

	public static void launchBootstrapperUtility(final String[] args){
		launch(applicationContext -> runBootstrapperUtility(args, applicationContext));
	}

	private static void runBootstrapperUtility(final String[] args, final ApplicationContext applicationContext){
		try {
			MaxwellBootstrapUtilityRunner maxwellBootstrapUtilityRunner = applicationContext.getBean(MaxwellBootstrapUtilityRunner.class);
			maxwellBootstrapUtilityRunner.run(args);
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
