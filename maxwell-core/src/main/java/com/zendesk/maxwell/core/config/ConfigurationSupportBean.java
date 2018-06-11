package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConfigurationSupportBean implements ConfigurationSupport {

	@Override
	public String fetchOption(String name, Properties properties, String defaultVal) {
		if (properties.containsKey(name) )
			return properties.getProperty(name);
		else
			return defaultVal;
	}

	@Override
	public boolean fetchBooleanOption(String name, Properties properties, boolean defaultVal) {
		if (properties.containsKey(name) )
			return Boolean.valueOf(properties.getProperty(name));
		else
			return defaultVal;
	}

	@Override
	public Long fetchLongOption(String name, Properties properties, Long defaultVal) {
		String strOption = fetchOption(name, properties, null);
		if ( strOption == null )
			return defaultVal;
		else {
			try {
				return Long.valueOf(strOption);
			} catch ( NumberFormatException e ) {
				throw new InvalidOptionException("Invalid value for " + name + ", integer required", "--" + name);
			}
		}
	}

	@Override
	public Integer fetchIntegerOption(String name, Properties properties, Integer defaultVal) {
		String strOption = fetchOption(name, properties, null);
		if ( strOption == null )
			return defaultVal;
		else {
			try {
				return Integer.valueOf(strOption);
			} catch ( NumberFormatException e ) {
				throw new InvalidOptionException("Invalid value for " + name + ", integer required", "--" + name);
			}
		}
	}
}
