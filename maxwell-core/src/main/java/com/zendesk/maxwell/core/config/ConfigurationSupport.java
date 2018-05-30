package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import joptsimple.OptionSet;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConfigurationSupport {
	public static final String DEFAULT_CONFIG_FILE = "config.properties";

	public String fetchOption(String name, OptionSet options, Properties properties, String defaultVal) {
		if ( options != null && options.has(name) )
			return (String) options.valueOf(name);
		else if ( (properties != null) && properties.containsKey(name) )
			return properties.getProperty(name);
		else
			return defaultVal;
	}

	public boolean fetchBooleanOption(String name, OptionSet options, Properties properties, boolean defaultVal) {
		if ( options != null && options.has(name) ) {
			if ( !options.hasArgument(name) )
				return true;
			else
				return Boolean.valueOf((String) options.valueOf(name));
		} else if ( (properties != null) && properties.containsKey(name) )
			return Boolean.valueOf(properties.getProperty(name));
		else
			return defaultVal;
	}

	public Long fetchLongOption(String name, OptionSet options, Properties properties, Long defaultVal) {
		String strOption = fetchOption(name, options, properties, null);
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

	public MaxwellMysqlConfig parseMysqlConfig(String prefix, OptionSet options, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host     = fetchOption(prefix + "host", options, properties, null);
		config.password = fetchOption(prefix + "password", options, properties, null);
		config.user     = fetchOption(prefix + "user", options, properties, null);
		config.port     = Integer.valueOf(fetchOption(prefix + "port", options, properties, "3306"));
		config.sslMode  = this.getSslModeFromString(fetchOption(prefix + "ssl", options, properties, null));
		config.setJDBCOptions(
				fetchOption(prefix + "jdbc_options", options, properties, null));
		return config;
	}

	private SSLMode getSslModeFromString(String sslMode) {
		if (sslMode != null) {
			for (SSLMode mode : SSLMode.values()) {
				if (mode.toString().equals(sslMode)) {
					return mode;
				}
			}
			System.err.println("Invalid binlog SSL mode string: " + sslMode);
			System.exit(1);
		}
		return null;
	}
}
