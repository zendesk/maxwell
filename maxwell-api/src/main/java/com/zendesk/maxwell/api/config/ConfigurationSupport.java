package com.zendesk.maxwell.api.config;

import java.util.Properties;

public interface ConfigurationSupport {
	String DEFAULT_CONFIG_FILE = "config.properties";

	String fetchOption(String name, Properties properties, String defaultVal);

	boolean fetchBooleanOption(String name, Properties properties, boolean defaultVal);

	Long fetchLongOption(String name, Properties properties, Long defaultVal);
}
