package com.zendesk.maxwell.standalone.config;

import java.util.Properties;

public interface ConfigurationOptionMerger {
	Properties mergeCommandLineOptionsWithConfigurationAndSystemEnvironment(String[] args);
}
