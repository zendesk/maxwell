package com.zendesk.maxwell.standalone.bootstrap;

import com.zendesk.maxwell.bootstrap.MaxwellBootstrapUtilityRunner;
import com.zendesk.maxwell.standalone.bootstrap.config.MaxwellBootstrapUtilityCommandLineOptions;
import com.zendesk.maxwell.config.InvalidOptionException;
import com.zendesk.maxwell.config.InvalidUsageException;
import com.zendesk.maxwell.core.config.InvalidOptionException;
import com.zendesk.maxwell.core.config.InvalidUsageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxwellBootstrapUtility {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtility.class);

	public static void main(String[] args) {
		try {
			new MaxwellBootstrapUtilityRunner().run(args);
		} catch (InvalidUsageException e){
			new MaxwellBootstrapUtilityCommandLineOptions().usage(e.getMessage());
		} catch (InvalidOptionException e){
			new MaxwellBootstrapUtilityCommandLineOptions().usageForOptions(e.getMessage(), e.getFilterOptions());
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			LOGGER.info("done.");
		}
	}

}
