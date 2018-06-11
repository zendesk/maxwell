package com.zendesk.maxwell.api.config;

public interface CommandLineOptionParserContext {
	void addOption(String name, String description);
	void addOptionWithRequiredArgument(String name, String description);
	void addOptionWithOptionalArgument(String name, String description);

	void addSeparator();
}
