package com.zendesk.maxwell.core.config;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public interface CommandLineOptions {
	OptionSet parse(String[] args);
	OptionParser getParser();
	void usage(String string);
	void usageForOptions(String string, String... filterOptions);
}
