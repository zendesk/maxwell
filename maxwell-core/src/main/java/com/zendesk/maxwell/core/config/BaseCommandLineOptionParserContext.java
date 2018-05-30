package com.zendesk.maxwell.core.config;

import joptsimple.OptionParser;

public class BaseCommandLineOptionParserContext implements CommandLineOptionParserContext {
	private static final String SEPARATOR_PREFIX = "__separator_";
	private final OptionParser parser;
	private int separatorCount;

	public BaseCommandLineOptionParserContext(OptionParser parser) {
		this.parser = parser;
		this.separatorCount = 1;
	}


	@Override
	public void addOption(String name, String description) {
		parser.accepts(name, description);
	}

	@Override
	public void addOptionWithRequiredArgument(String name, String description) {
		parser.accepts(name, description).withRequiredArg();
	}

	@Override
	public void addOptionWithOptionalArgument(String name, String description) {
		parser.accepts(name, description).withOptionalArg();
	}

	@Override
	public void addSeparator() {
		parser.accepts(SEPARATOR_PREFIX + separatorCount);
	}
}
