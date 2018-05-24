package com.zendesk.maxwell.standalone;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.zendesk.maxwell.MaxwellRunner;
import com.zendesk.maxwell.config.*;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.InvalidUsageException;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class Maxwell {

	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);


	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();
			MaxwellConfig config = new MaxwellConfigFactory().createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(args);

			if ( config.log_level != null )
				Logging.setLevel(config.log_level);

			final MaxwellRunner maxwell = new MaxwellRunner(config);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					maxwell.terminate();
					StaticShutdownCallbackRegistry.invoke();
				}
			});

			maxwell.start();
		} catch ( SQLException e ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch ( URISyntaxException e ) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			LOGGER.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
			LOGGER.error("URISyntaxException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch (InvalidUsageException e){
			new com.zendesk.maxwell.standalone.config.MaxwellCommandLineOptions().usage(e.getMessage());
		} catch (InvalidOptionException e){
			new com.zendesk.maxwell.standalone.config.MaxwellCommandLineOptions().usageForOptions(e.getMessage(), e.getFilterOptions());
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
