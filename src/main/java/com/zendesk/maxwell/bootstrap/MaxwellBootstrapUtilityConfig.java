package com.zendesk.maxwell.bootstrap;

import joptsimple.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Map;

import com.zendesk.maxwell.util.AbstractConfig;
import com.zendesk.maxwell.MaxwellMysqlConfig;

public class MaxwellBootstrapUtilityConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtilityConfig.class);

	public MaxwellMysqlConfig mysql;
	public MaxwellMysqlConfig replicationMysql;
	public String  databaseName;
	public String  schemaDatabaseName;
	public String  tableName;
	public String  whereClause;
	public String  log_level;
	public String  clientID;
	public String  comment;

	public Long    abortBootstrapID;
	public Long    monitorBootstrapID;

	public MaxwellBootstrapUtilityConfig(String argv[]) {
		this.parse(argv);
		this.setDefaults();
	}


	public String getConnectionURI() {
		try {
			return getConfigConnectionURI(mysql);
		} catch (URISyntaxException e) {
			LOGGER.error(e.getMessage(), e);
			throw new RuntimeException("Unable to generate bootstrap's jdbc connection URI", e);
		}
	}

	public String getReplicationConnectionURI( ) {
		try {
			return getConfigReplicationConnectionURI(replicationMysql);
		} catch (URISyntaxException e) {
			LOGGER.error(e.getMessage(), e);
			throw new RuntimeException("Unable to generate bootstrap's replication jdbc connection URI", e);
		}
	}

	private String getConfigConnectionURI(MaxwellMysqlConfig config) throws URISyntaxException {
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme("jdbc:mysql");
		uriBuilder.setHost(config.host);
		uriBuilder.setPort(config.port);
		uriBuilder.setPath("/" + schemaDatabaseName);
		for (Map.Entry<String, String> jdbcOption : config.jdbcOptions.entrySet()) {
			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
		}
		return uriBuilder.build().toString();
	}

	private String getConfigReplicationConnectionURI(MaxwellMysqlConfig config) throws URISyntaxException {
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme("jdbc:mysql");
		uriBuilder.setHost(config.host);
		uriBuilder.setPort(config.port);
		uriBuilder.setPath("/" + databaseName);
		for (Map.Entry<String, String> jdbcOption : config.jdbcOptions.entrySet()) {
			uriBuilder.addParameter(jdbcOption.getKey(), jdbcOption.getValue());
		}
		return uriBuilder.build().toString();
	}

	protected OptionParser buildOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config.properties file" )
				.withRequiredArg();
		parser.accepts( "env_config", "json object encoded config in an environment variable" )
				.withRequiredArg();
		parser.accepts( "__separator_1", "" );
		parser.accepts( "database", "database that contains the table to bootstrap").withRequiredArg();
		parser.accepts( "table", "table to bootstrap").withRequiredArg();
		parser.accepts( "where", "where clause to restrict the rows bootstrapped from the specified table. e.g. my_date >= '2017-01-01 11:07:13'").withOptionalArg();
		parser.accepts( "__separator_2", "" );
		parser.accepts( "abort", "bootstrap_id to abort" ).withRequiredArg();
		parser.accepts( "monitor", "bootstrap_id to monitor" ).withRequiredArg();
		parser.accepts( "__separator_3", "" );
		parser.accepts( "client_id", "maxwell client to perform the bootstrap" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR. default: WARN" ).withRequiredArg();
		parser.accepts( "__separator_4", "" );
		parser.accepts( "host", "mysql host containing 'maxwell' database. default: localhost").withRequiredArg();
		parser.accepts( "user", "mysql username. default: maxwell" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withOptionalArg();
		parser.accepts( "port", "mysql port. default: 3306" ).withRequiredArg();
		parser.accepts( "__separator_5", "" );
		parser.accepts( "replication_host", "mysql host to query. default: (host)").withRequiredArg();
		parser.accepts( "replication_user", "username. default: maxwell" ).withRequiredArg();
		parser.accepts( "replication_password", "password" ).withOptionalArg();
		parser.accepts( "replication_port", "port. default: 3306" ).withRequiredArg();
		parser.accepts( "__separator_6", "" );
		parser.accepts( "comment", "arbitrary comment to be added to every bootstrap row record" ).withRequiredArg();
		parser.accepts( "schema_database", "database that contains maxwell schema and state").withRequiredArg();
		parser.accepts( "help", "display help").forHelp();

		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				return output.replaceAll("--__separator_.*", "");
			}
		};
		parser.formatHelpWith(helpFormatter);
		return parser;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			usage("unknown log level: " + level);
		return level;
	}

	private void parse(String [] argv) {
		OptionSet options = buildOptionParser().parse(argv);

		final Properties properties;

		if (options.has("config")) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if (options.has("env_config")) {
			Properties envConfigProperties = readPropertiesEnv((String) options.valueOf("env_config"));
			for (Map.Entry<Object, Object> entry : envConfigProperties.entrySet()) {
				Object key = entry.getKey();
				if (properties.put(key, entry.getValue()) != null) {
					LOGGER.debug("Replaced config key {} with value from env_config", key);
				}
			}
		}
		if ( options.has("help") )
			usage("Help for Maxwell Bootstrap Utility:\n\nPlease provide one of:\n--database AND --table, --abort ID, or --monitor ID");

		if ( options.has("log_level"))
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));

		this.mysql = parseMysqlConfig("", options, properties);
		if ( this.mysql.host == null )
			this.mysql.host = "localhost";

		this.replicationMysql = parseMysqlConfig("replication_", options, properties);
		if ( replicationMysql.host == null && replicationMysql.port.equals(mysql.port) ) {
			this.replicationMysql = this.mysql;
		} else {
			if ( replicationMysql.user == null )
				replicationMysql.user = mysql.user;

			if ( replicationMysql.password == null )
				replicationMysql.password = mysql.password;
		}

		this.schemaDatabaseName = fetchStringOption("schema_database", options, properties, "maxwell");
		this.clientID = fetchStringOption("client_id", options, properties, "maxwell");

        if ( options.has("database") )
			this.databaseName = (String) options.valueOf("database");
		else if ( !options.has("abort") && !options.has("monitor") )
			usage("please specify a database");

		if ( options.has("abort") )
			this.abortBootstrapID = Long.valueOf((String) options.valueOf("abort"));
		else if ( options.has("monitor") )
			this.monitorBootstrapID = Long.valueOf((String) options.valueOf("monitor"));

		if ( this.abortBootstrapID != null ) {
			if ( this.monitorBootstrapID != null )
				usage("--abort is incompatible with --monitor");
			if ( this.databaseName != null )
				usage("--abort is incompatible with --database and --table");
		}

		if ( this.monitorBootstrapID != null ) {
			if ( this.databaseName != null )
				usage("--monitor is incompatible with --database and --table");
		}

		if ( options.has("table") )
			this.tableName = (String) options.valueOf("table");
		else if ( !options.has("abort") && !options.has("monitor") )
			usage("please specify a table");

		if ( options.has("where")  && !StringUtils.isEmpty(((String) options.valueOf("where"))) )
			this.whereClause = (String) options.valueOf("where");

		if ( options.has("comment") )
			this.comment = (String) options.valueOf("comment");
	}

	private Properties parseFile(String filename, boolean abortOnMissing) {
		return this.readPropertiesFile(filename, abortOnMissing);
	}


	private void setDefaults() {
		if ( this.log_level == null ) {
			this.log_level = "WARN";
		}
	}
}
