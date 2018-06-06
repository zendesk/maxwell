package com.zendesk.maxwell.core.bootstrap.config;

import com.zendesk.maxwell.api.config.InvalidUsageException;
import com.zendesk.maxwell.core.config.ConfigurationFileParser;
import com.zendesk.maxwell.core.config.ConfigurationSupport;
import joptsimple.OptionSet;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class MaxwellBootstrapUtilityConfigFactory {

	private final MaxwellBootstrapUtilityCommandLineOptions maxwellBootstrapUtilityCommandLineOptions;
	private final ConfigurationFileParser configurationFileParser;
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public MaxwellBootstrapUtilityConfigFactory(MaxwellBootstrapUtilityCommandLineOptions maxwellBootstrapUtilityCommandLineOptions, ConfigurationFileParser configurationFileParser, ConfigurationSupport configurationSupport) {
		this.maxwellBootstrapUtilityCommandLineOptions = maxwellBootstrapUtilityCommandLineOptions;
		this.configurationFileParser = configurationFileParser;
		this.configurationSupport = configurationSupport;
	}

	public MaxwellBootstrapUtilityConfig createConfigurationFromArgumentsAndConfigurationFile(String [] argv) {
		OptionSet options = maxwellBootstrapUtilityCommandLineOptions.parse(argv);
		Properties properties;

		if ( options.has("config") ) {
			properties = configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			properties = configurationFileParser.parseFile(ConfigurationSupport.DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			throw new InvalidUsageException("Help for Maxwell Bootstrap Utility:\n\nPlease provide one of:\n--database AND --table, --abort ID, or --monitor ID");

		return createConfigFrom(options, properties);
	}

	private MaxwellBootstrapUtilityConfig createConfigFrom(OptionSet options, Properties properties) {
		MaxwellBootstrapUtilityConfig config = new MaxwellBootstrapUtilityConfig();
		if ( options.has("log_level"))
			config.log_level = parseLogLevel((String) options.valueOf("log_level"));

		config.mysql = configurationSupport.parseMysqlConfig("", properties);
		if ( config.mysql.getHost() == null )
			config.mysql.setHost("localhost");

		config.schemaDatabaseName = configurationSupport.fetchOption("schema_database", properties, "maxwell");

		if ( options.has("database") )
			config.databaseName = (String) options.valueOf("database");
		else if ( !options.has("abort") && !options.has("monitor") )
			throw new InvalidUsageException("please specify a database");

		if ( options.has("abort") )
			config.abortBootstrapID = Long.valueOf((String) options.valueOf("abort"));
		else if ( options.has("monitor") )
			config.monitorBootstrapID = Long.valueOf((String) options.valueOf("monitor"));

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

		if ( options.has("table") )
			config.tableName = (String) options.valueOf("table");
		else if ( !options.has("abort") && !options.has("monitor") )
			throw new InvalidUsageException("please specify a table");

		if ( options.has("where")  && !StringUtils.isEmpty(((String) options.valueOf("where"))) )
			config.whereClause = (String) options.valueOf("where");

		if ( options.has("client_id") )
			config.clientID = (String) options.valueOf("client_id");
		return config;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			throw new InvalidUsageException("unknown log level: " + level);
		return level;
	}

}
