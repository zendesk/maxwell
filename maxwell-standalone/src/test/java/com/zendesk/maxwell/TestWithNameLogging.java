package com.zendesk.maxwell;

import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ben on 8/31/16.
 */
public class TestWithNameLogging {
	private static Logger logger = LoggerFactory.getLogger("TestHarness");

	@Rule
	public TestWatcher watchman = new TestWatcher() {
		@Override
		public void starting(final Description method) {
			logger.info("========== BEGIN: " + method.getMethodName());
		}

		@Override
		protected void finished(Description method) {
			logger.info("========== END: " + method.getMethodName());
		}
	};
}
