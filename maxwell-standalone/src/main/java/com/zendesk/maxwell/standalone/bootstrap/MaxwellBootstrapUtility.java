package com.zendesk.maxwell.standalone.bootstrap;

import com.zendesk.maxwell.standalone.SpringLauncher;
import com.zendesk.maxwell.standalone.config.MaxwellBootstrapUtilityCommandLineOptions;
import com.zendesk.maxwell.standalone.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellBootstrapUtility {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtility.class);
	private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler(LOGGER, new MaxwellBootstrapUtilityCommandLineOptions());

	public static void main(String[] args) {
		try {
			SpringLauncher.launchBootstrapperUtility(args);
		} catch ( Exception e ) {
			EXCEPTION_HANDLER.handleException(e);
			System.exit(1);
		} finally {
			LOGGER.info("done.");
		}
	}

}
