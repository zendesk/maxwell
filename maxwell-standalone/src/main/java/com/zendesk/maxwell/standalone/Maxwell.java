package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.standalone.config.MaxwellCommandLineOptions;
import com.zendesk.maxwell.core.util.Logging;
import com.zendesk.maxwell.standalone.spring.SpringLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Maxwell {

	private static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);
	private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler(LOGGER, MaxwellCommandLineOptions.getInstance());

	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();
			SpringLauncher.launchMaxwell(args);
		} catch ( Exception e ) {
			EXCEPTION_HANDLER.handleException(e);
			System.exit(1);
		}
	}

}
