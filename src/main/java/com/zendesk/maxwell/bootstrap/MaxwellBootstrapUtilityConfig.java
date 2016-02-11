package com.zendesk.maxwell.bootstrap;

import joptsimple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

import java.io.IOException;
import com.zendesk.maxwell.util.AbstractConfig;

public class MaxwellBootstrapUtilityConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtilityConfig.class);

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;
	public String  databaseName;
	public String  schemaDatabaseName;
	public String  tableName;
	public String  log_level;

	public Long    abortBootstrapID;
	public Long    monitorBootstrapID;

	public MaxwellBootstrapUtilityConfig(String argv[]) {
		this.parse(argv);
		this.setDefaults();
	}

	public String getConnectionURI( ) {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + schemaDatabaseName;
	}

	protected OptionParser buildOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "__separator_1", "" );
		parser.accepts( "database", "database that contains the table to bootstrap").withRequiredArg();
		parser.accepts( "table", "table to bootstrap").withRequiredArg();
		parser.accepts( "__separator_2", "" );
		parser.accepts( "abort", "bootstrap_id to abort" ).withRequiredArg();
		parser.accepts( "monitor", "bootstrap_id to monitor" ).withRequiredArg();
		parser.accepts( "__separator_3", "" );
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR. default: WARN" ).withRequiredArg();
		parser.accepts( "host", "mysql host. default: localhost").withRequiredArg();
		parser.accepts( "user", "mysql username. default: maxwell" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port. default: 3306" ).withRequiredArg();
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

		if ( options.has("config") ) {
			parseFile((String) options.valueOf("config"), true);
		} else {
			parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			usage("Help for Maxwell Bootstrap Utility:\n\nPlease provide one of:\n--database AND --table, --abort ID, or --monitor ID");

		if ( options.has("log_level"))
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));

		if ( options.has("host"))
			this.mysqlHost = (String) options.valueOf("host");

		if ( options.has("user"))
			this.mysqlUser = (String) options.valueOf("user");

		if ( options.has("password"))
			this.mysqlPassword = (String) options.valueOf("password");

		if ( options.has("port"))
			this.mysqlPort = Integer.valueOf((String) options.valueOf("port"));

		if ( options.has("schema_database"))
			this.schemaDatabaseName = (String) options.valueOf("schema_database");

		if ( options.has("database") )
			this.databaseName = (String) options.valueOf("database");
		else if ( !options.has("abort") && !options.has("monitor") )
			usage("please specify a database");

		if ( options.has("abort") ) {
			this.abortBootstrapID = Long.valueOf((String) options.valueOf("abort"));
		}

		if ( options.has("monitor") )
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
	}

	private void parseFile(String filename, boolean abortOnMissing) {
		Properties p = this.readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			return;

		this.mysqlHost = p.getProperty("host");
		this.mysqlUser = p.getProperty("user", "maxwell");
		this.mysqlPort = Integer.valueOf(p.getProperty("port", "3306"));
		this.mysqlPassword = p.getProperty("password");
		this.schemaDatabaseName = p.getProperty("schema_database", "maxwell");
	}


	private void setDefaults() {

		if ( this.log_level == null ) {
			this.log_level = "WARN";
		}

		if ( this.mysqlHost == null ) {
			LOGGER.warn("mysql host not specified, defaulting to localhost");
			this.mysqlHost = "localhost";
		}
	}
}
