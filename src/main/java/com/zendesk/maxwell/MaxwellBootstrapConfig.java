package com.zendesk.maxwell;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class MaxwellBootstrapConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapConfig.class);

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;
	public String  databaseName;
	public String  tableName;
	public String log_level;

	public MaxwellBootstrapConfig(String argv[]) {
		this.parse(argv);
		this.setDefaults();
	}

	public String getConnectionURI( ) {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
	}

	private OptionParser getOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR. default: WARN" ).withRequiredArg();
		parser.accepts( "host", "mysql host. default: localhost").withRequiredArg();
		parser.accepts( "user", "mysql username. default: maxwell" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port. default: 3306" ).withRequiredArg();
		parser.accepts( "database", "database that contains the table to bootstrap").withRequiredArg();
		parser.accepts( "table", "table to bootstrap").withRequiredArg();
		parser.accepts( "help", "display help").forHelp();
		parser.formatHelpWith(new BuiltinHelpFormatter(160, 4));
		return parser;
	}

	private String parseLogLevel(String level) {
		level = level.toLowerCase();
		if ( !( level.equals("debug") || level.equals("info") || level.equals("warn") || level.equals("error")))
			usage("unknown log level: " + level);
		return level;
	}

	private void parse(String [] argv) {
		OptionSet options = getOptionParser().parse(argv);

		if ( options.has("help") )
			usage("Help for Maxwell Bootstrap Utility:");

		if ( options.has("log_level"))
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));

		if ( options.has("host"))
			this.mysqlHost = (String) options.valueOf("host");

		if ( options.has("user"))
			this.mysqlUser = (String) options.valueOf("user");

		if ( options.has("password"))
			this.mysqlPassword = (String) options.valueOf("password");
		else
			usage("mysql password not given!");

		if ( options.has("port"))
			this.mysqlPort = Integer.valueOf((String) options.valueOf("port"));

		if ( options.has("database") )
			this.databaseName = (String) options.valueOf("database");
		else
			usage("please specify a database");

		if ( options.has("table") )
			this.tableName = (String) options.valueOf("table");
		else
			usage("please specify a table");

	}

	private void setDefaults() {

		if ( this.log_level == null ) {
			this.log_level = "WARN";
		}

		if ( this.mysqlHost == null ) {
			LOGGER.warn("mysql host not specified, defaulting to localhost");
			this.mysqlHost = "localhost";
		}

		if ( this.mysqlUser == null ) {
			this.mysqlUser = "maxwell";
		}

		if ( this.mysqlPort == null ) {
			this.mysqlPort = 3306;
		}

	}

	private void usage(String string) {
		System.err.println(string);
		System.err.println();
		try {
			getOptionParser().printHelpOn(System.err);
			System.exit(1);
		} catch (IOException e) {
		}
	}

}
