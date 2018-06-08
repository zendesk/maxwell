package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.MaxwellMysqlConfig;
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

	public BaseMaxwellMysqlConfig parseMysqlConfig(String prefix, Properties properties) {
		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();
		config.setHost(configurationSupport.fetchOption(prefix + "host", properties, null));
		config.setPassword(configurationSupport.fetchOption(prefix + "password", properties, null));
		config.setUser(configurationSupport.fetchOption(prefix + "user", properties, null));
		config.setPort(configurationSupport.fetchIntegerOption(prefix + "port", properties, MaxwellMysqlConfig.DEFAULT_MYSQL_PORT));
		config.setSslMode(this.getSslModeFromString(configurationSupport.fetchOption(prefix + "ssl", properties, null)));
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
			System.err.println("Invalid binlog SSL mode string: " + sslMode);
			System.exit(1);
		}
		return null;
	}
}
