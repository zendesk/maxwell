package com.zendesk.maxwell.standalone.bootstrap.config;

import com.zendesk.maxwell.bootstrap.config.MaxwellBootstrapUtilityConfig;
import com.zendesk.maxwell.standalone.config.AbstractConfigurationFactory;
import com.zendesk.maxwell.config.ConfigurationFileParser;
import com.zendesk.maxwell.config.InvalidUsageException;
import joptsimple.OptionSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

public class MaxwellBootstrapUtilityConfigFactory extends AbstractConfigurationFactory {

	private final MaxwellBootstrapUtilityCommandLineOptions maxwellBootstrapUtilityCommandLineOptions;
	private final ConfigurationFileParser configurationFileParser;

	public MaxwellBootstrapUtilityConfigFactory(){
		this(new MaxwellBootstrapUtilityCommandLineOptions(), new ConfigurationFileParser());
	}

	public MaxwellBootstrapUtilityConfigFactory(MaxwellBootstrapUtilityCommandLineOptions maxwellBootstrapUtilityCommandLineOptions, ConfigurationFileParser configurationFileParser) {
		this.maxwellBootstrapUtilityCommandLineOptions = maxwellBootstrapUtilityCommandLineOptions;
		this.configurationFileParser = configurationFileParser;
	}

	public MaxwellBootstrapUtilityConfig createConfigurationFromArgumentsAndConfigurationFile(String [] argv) {
		OptionSet options = maxwellBootstrapUtilityCommandLineOptions.createParser().parse(argv);
		Properties properties;

		if ( options.has("config") ) {
			properties = configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			properties = configurationFileParser.parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			throw new InvalidUsageException("Help for Maxwell Bootstrap Utility:\n\nPlease provide one of:\n--database AND --table, --abort ID, or --monitor ID");

		return createConfigFrom(options, properties);
	}

	private MaxwellBootstrapUtilityConfig createConfigFrom(OptionSet options, Properties properties) {
		MaxwellBootstrapUtilityConfig config = new MaxwellBootstrapUtilityConfig();
		if ( options.has("log_level"))
			config.log_level = parseLogLevel((String) options.valueOf("log_level"));

		config.mysql = parseMysqlConfig("", options, properties);
		if ( config.mysql.host == null )
			config.mysql.host = "localhost";

		config.schemaDatabaseName = fetchOption("schema_database", options, properties, "maxwell");

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
