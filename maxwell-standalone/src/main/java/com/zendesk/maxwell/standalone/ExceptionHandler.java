package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.core.LauncherException;
import com.zendesk.maxwell.core.config.AbstractCommandLineOptions;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.InvalidUsageException;
import com.zendesk.maxwell.core.producer.ProducerInstantiationException;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class ExceptionHandler {
	private final Logger logger;
	private final AbstractCommandLineOptions commandLineOptions;

	public ExceptionHandler(Logger logger, AbstractCommandLineOptions commandLineOptions) {
		this.logger = logger;
		this.commandLineOptions = commandLineOptions;
	}

	public void handleException(Exception e){
		Throwable finalException = unwrap(e);
		if ( finalException instanceof SQLException) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			logger.error("SQLException: " + finalException.getLocalizedMessage());
		} else if ( finalException instanceof URISyntaxException) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			logger.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
			logger.error("URISyntaxException: " + finalException.getLocalizedMessage());
		} else if (finalException instanceof InvalidUsageException){
			commandLineOptions.usage(finalException.getLocalizedMessage());
		} else if (finalException instanceof InvalidOptionException){
			commandLineOptions.usageForOptions(finalException.getLocalizedMessage(), ((InvalidOptionException) finalException).getFilterOptions());
		} else {
			finalException.printStackTrace();
		}
	}

	private Throwable unwrap(Exception e){
		return isWrapperException(e) ? e.getCause() : e;
	}

	private boolean isWrapperException(Exception e) {
		return e instanceof LauncherException || e instanceof ProducerInstantiationException;
	}
}
