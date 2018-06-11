package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.api.config.InvalidOptionException;
import com.zendesk.maxwell.api.config.MaxwellMysqlConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

import static com.zendesk.maxwell.api.config.MaxwellMysqlConfig.*;

@Service
public class MySqlConfigurationSupport {
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public MySqlConfigurationSupport(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	public BaseMaxwellMysqlConfig parseMysqlConfig(String prefix, Properties properties) {
		BaseMaxwellMysqlConfig config = new BaseMaxwellMysqlConfig();
		config.setHost(configurationSupport.fetchOption(prefix + CONFIGURATION_OPTION_HOST, properties, null));
		config.setPassword(configurationSupport.fetchOption(prefix + CONFIGURATION_OPTION_PASSWORD, properties, null));
		config.setUser(configurationSupport.fetchOption(prefix + CONFIGURATION_OPTION_USER, properties, null));
		config.setPort(configurationSupport.fetchIntegerOption(prefix + CONFIGURATION_OPTION_PORT, properties, MaxwellMysqlConfig.DEFAULT_MYSQL_PORT));
		config.setSslMode(this.getSslModeFromString(configurationSupport.fetchOption(prefix + CONFIGURATION_OPTION_SSL, properties, null)));
		config.setJDBCOptions(configurationSupport.fetchOption(prefix + CONFIGURATION_OPTION_JDBC_OPTIONS, properties, null));
		return config;
	}

	private SSLMode getSslModeFromString(String sslMode) {
		if (sslMode != null) {
			for (SSLMode mode : SSLMode.values()) {
				if (mode.toString().equals(sslMode)) {
					return mode;
				}
			}
			throw new InvalidOptionException("Invalid binlog SSL mode string: " + sslMode, "--" + CONFIGURATION_OPTION_SSL);
		}
		return null;
	}
}
