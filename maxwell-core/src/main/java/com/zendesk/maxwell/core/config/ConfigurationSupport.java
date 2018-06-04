package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConfigurationSupport {
	public static final String DEFAULT_CONFIG_FILE = "config.properties";

	public String fetchOption(String name, Properties properties, String defaultVal) {
		if (properties.containsKey(name) )
			return properties.getProperty(name);
		else
			return defaultVal;
	}

	public boolean fetchBooleanOption(String name, Properties properties, boolean defaultVal) {
		if (properties.containsKey(name) )
			return Boolean.valueOf(properties.getProperty(name));
		else
			return defaultVal;
	}

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

	public MaxwellMysqlConfig parseMysqlConfig(String prefix, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host     = fetchOption(prefix + "host", properties, null);
		config.password = fetchOption(prefix + "password", properties, null);
		config.user     = fetchOption(prefix + "user", properties, null);
		config.port     = Integer.valueOf(fetchOption(prefix + "port", properties, "3306"));
		config.sslMode  = this.getSslModeFromString(fetchOption(prefix + "ssl", properties, null));
		config.setJDBCOptions(fetchOption(prefix + "jdbc_options", properties, null));
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
