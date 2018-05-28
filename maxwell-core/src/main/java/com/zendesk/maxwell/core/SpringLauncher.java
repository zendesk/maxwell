package com.zendesk.maxwell.core;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.core.bootstrap.MaxwellBootstrapUtilityRunner;
import com.zendesk.maxwell.core.bootstrap.config.MaxwellBootstrapUtilityConfigFactory;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellConfigFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SpringLauncher {

	public static void launchMaxwell(final String[] args){
		launchMaxwell(args, Optional.empty());
	}

	public static void launchMaxwell(final String[] args, final BiConsumer<MaxwellConfig,ApplicationContext> beforeStartEventHandler){
		launchMaxwell(args, Optional.of(beforeStartEventHandler));
	}

	private static void launchMaxwell(final String[] args, final Optional<BiConsumer<MaxwellConfig,ApplicationContext>> beforeStartEventHandler){
		launch((applicationContext -> runMaxwell(args, beforeStartEventHandler, applicationContext)));
	}

	private static void runMaxwell(final String[] args, final Optional<BiConsumer<MaxwellConfig,ApplicationContext>> beforeStartEventHandler, final ApplicationContext applicationContext) {
		try {
			final MaxwellConfigFactory maxwellConfigFactory = applicationContext.getBean(MaxwellConfigFactory.class);
			final MaxwellContextFactory maxwellContextFactory = applicationContext.getBean(MaxwellContextFactory.class);
			final MaxwellRunner maxwellRunner = applicationContext.getBean(MaxwellRunner.class);

			final MaxwellConfig config = maxwellConfigFactory.createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(args);
			beforeStartEventHandler.ifPresent((c) -> c.accept(config, applicationContext));

			final MaxwellContext context = maxwellContextFactory.createFor(config);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				maxwellRunner.terminate(context);
				StaticShutdownCallbackRegistry.invoke();
			}));

			maxwellRunner.start(context);
		}catch (Exception e){
			throw new LauncherException("Error while running Maxwell", e);
		}
	}

	public static void launchBootstrapperUtility(final String[] args){
		launch(applicationContext -> runBootstrapperUtility(args, applicationContext));
	}

	private static void runBootstrapperUtility(final String[] args, final ApplicationContext applicationContext){
		try {
			MaxwellBootstrapUtilityConfigFactory maxwellBootstrapUtilityConfigFactory = applicationContext.getBean(MaxwellBootstrapUtilityConfigFactory.class);
			MaxwellBootstrapUtilityRunner maxwellBootstrapUtilityRunner = new MaxwellBootstrapUtilityRunner(maxwellBootstrapUtilityConfigFactory);
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