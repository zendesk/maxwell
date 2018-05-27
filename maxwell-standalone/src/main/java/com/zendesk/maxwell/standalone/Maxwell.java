package com.zendesk.maxwell.standalone;

import com.zendesk.maxwell.core.*;
import com.zendesk.maxwell.core.config.MaxwellCommandLineOptions;
import com.zendesk.maxwell.core.util.Logging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Maxwell {

	private static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);
	private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler(LOGGER, new MaxwellCommandLineOptions());


	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();
			SpringLauncher.launchMaxwell(args, (config,ctx) -> {
				if ( config.log_level != null ){
					Logging.setLevel(config.log_level);
				}
			});
		} catch ( Exception e ) {
			EXCEPTION_HANDLER.handleException(e);
			System.exit(1);
		}
	}

}
