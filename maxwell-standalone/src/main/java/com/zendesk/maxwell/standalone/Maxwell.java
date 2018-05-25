package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.*;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.InvalidUsageException;
import com.zendesk.maxwell.core.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class Maxwell {

	private static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);


	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();
			SpringLauncher.launchMaxwell(args, (config,ctx) -> {
				if ( config.log_level != null ){
					Logging.setLevel(config.log_level);
				}
			});
		} catch ( LauncherException e ) {
			handleException(e);
			System.exit(1);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void handleException(LauncherException e){
		Throwable cause = e.getCause();
		if ( cause instanceof SQLException ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + cause.getLocalizedMessage());
		} else if ( cause instanceof URISyntaxException ) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			LOGGER.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
			LOGGER.error("URISyntaxException: " + cause.getLocalizedMessage());
		} else if (cause instanceof InvalidUsageException){
			new MaxwellCommandLineOptions().usage(cause.getLocalizedMessage());
		} else if (cause instanceof InvalidOptionException){
			new MaxwellCommandLineOptions().usageForOptions(cause.getLocalizedMessage(), ((InvalidOptionException) cause).getFilterOptions());
		} else {
			cause.printStackTrace();
		}
	}
}
