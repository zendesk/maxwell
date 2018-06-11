package com.zendesk.maxwell.api.config;

import java.util.Properties;

public interface ConfigurationSupport {

	String fetchOption(String name, Properties properties, String defaultVal);

	boolean fetchBooleanOption(String name, Properties properties, boolean defaultVal);

	Long fetchLongOption(String name, Properties properties, Long defaultVal);

	Integer fetchIntegerOption(String name, Properties properties, Integer defaultVal);
}
