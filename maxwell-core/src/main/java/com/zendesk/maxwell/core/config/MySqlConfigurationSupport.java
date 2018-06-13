package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MySqlConfigurationSupport {
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public MySqlConfigurationSupport(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	public MaxwellMysqlConfig parseMysqlConfig(String prefix, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host = configurationSupport.fetchOption(prefix + "host", properties, null);
		config.password = configurationSupport.fetchOption(prefix + "password", properties, null);
		config.user = configurationSupport.fetchOption(prefix + "user", properties, null);
		config.port = configurationSupport.fetchIntegerOption(prefix + "port", properties, MaxwellMysqlConfig.DEFAULT_MYSQL_PORT);
		config.sslMode = this.getSslModeFromString(configurationSupport.fetchOption(prefix + "ssl", properties, null));
		config.setJDBCOptions(configurationSupport.fetchOption(prefix + "jdbc_options", properties, null));
		return config;
	}

	private SSLMode getSslModeFromString(String sslMode) {
		if (sslMode != null) {
			for (SSLMode mode : SSLMode.values()) {
				if (mode.toString().equals(sslMode)) {
					return mode;
				}
			}
			throw new InvalidOptionException("Invalid binlog SSL mode string: " + sslMode, "--ssl");
		}
		return null;
	}
}
