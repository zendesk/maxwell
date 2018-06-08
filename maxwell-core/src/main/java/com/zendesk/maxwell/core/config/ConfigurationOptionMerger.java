package com.zendesk.maxwell.core.config;

import java.util.Properties;

public interface ConfigurationOptionMerger {
	Properties mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(String[] args);
}
