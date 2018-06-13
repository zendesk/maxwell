package com.zendesk.maxwell.core.bootstrap.config;

import com.zendesk.maxwell.api.config.InvalidUsageException;
import com.zendesk.maxwell.api.config.ConfigurationSupport;
import com.zendesk.maxwell.core.config.MySqlConfigurationSupport;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MaxwellBootstrapUtilityConfigFactory {

	private final ConfigurationSupport configurationSupport;
	private final MySqlConfigurationSupport mySqlConfigurationSupport;

	@Autowired
	public MaxwellBootstrapUtilityConfigFactory(ConfigurationSupport configurationSupport, MySqlConfigurationSupport mySqlConfigurationSupport) {
		this.configurationSupport = configurationSupport;
		this.mySqlConfigurationSupport = mySqlConfigurationSupport;
	}

	public MaxwellBootstrapUtilityConfig createFor(Properties properties) {
		MaxwellBootstrapUtilityConfig config = new MaxwellBootstrapUtilityConfig();
		if ( properties.containsKey("log_level"))
			config.log_level = parseLogLevel(properties.getProperty("log_level"));

		config.mysql = mySqlConfigurationSupport.parseMysqlConfig("", properties);
		if ( config.mysql.host == null )
			config.mysql.host = "localhost";

		config.schemaDatabaseName = configurationSupport.fetchOption("schema_database", properties, "maxwell");

		if ( properties.containsKey("database") )
			config.databaseName = properties.getProperty("database");
		else if ( !properties.containsKey("abort") && !properties.containsKey("monitor") )
			throw new InvalidUsageException("please specify a database");

		if ( properties.containsKey("abort") )
			config.abortBootstrapID = Long.valueOf(properties.getProperty("abort"));
		else if ( properties.containsKey("monitor") )
			config.monitorBootstrapID = Long.valueOf(properties.getProperty("monitor"));

		if ( config.abortBootstrapID != null ) {
			if ( config.monitorBootstrapID != null )
				throw new InvalidUsageException("--abort is incompatible with --monitor");
			if ( config.databaseName != null )
				throw new InvalidUsageException("--abort is incompatible with --database and --table");
		}

		if ( config.monitorBootstrapID != null ) {
			if ( config.databaseName != null )
				throw new InvalidUsageException("--monitor is incompatible with --database and --table");
		}

		if ( properties.containsKey("table") )
			config.tableName = properties.getProperty("table");
		else if ( !properties.containsKey("abort") && !properties.containsKey("monitor") )
			throw new InvalidUsageException("please specify a table");

		if ( properties.containsKey("where")  && !StringUtils.isEmpty((properties.getProperty("where"))) )
			config.whereClause = properties.getProperty("where");

		if ( properties.containsKey("client_id") )
			config.clientID = properties.getProperty("client_id");
		return config;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			throw new InvalidUsageException("unknown log level: " + level);
		return level;
	}

}
