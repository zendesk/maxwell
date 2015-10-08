package com.zendesk.maxwell;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.SchemaStore;

public class MaxwellConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);
	static final String DEFAULT_CONFIG_FILE = "config.properties";

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;
    public String include_databases,exclude_databases,include_tables,exclude_tables;
	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String producerType;
	public String outputFile;
	public String log_level;

	public Integer maxSchemas;

	public MaxwellConfig() { // argv is only null in tests
		this.kafkaProperties = new Properties();
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
		this.setDefaults();
	}


	public String getConnectionURI() {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
	}

	private OptionParser getOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();
		parser.accepts( "host", "mysql host" ).withRequiredArg();
		parser.accepts( "user", "mysql username" ).withRequiredArg();
		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port" ).withRequiredArg();
		parser.accepts( "producer", "producer type: stdout|file|kafka" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withOptionalArg();
		parser.accepts( "max_schemas", "how many old schema definitions maxwell should keep around.  default: 5").withOptionalArg();
		parser.accepts( "include_databases", "include thes databases, formatted as include_databases=db1,db2").withOptionalArg();
		parser.accepts( "exclude_databases", "exclude thes databases, formatted as exclude_databases=db1,db2").withOptionalArg();
		parser.accepts( "include_tables", "include thes tables, formatted as include_tables=db1,db2").withOptionalArg();
		parser.accepts( "exclude_tables", "exclude thes tables, formatted as exclude_tables=tb1,tb2").withOptionalArg();
		
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

		if ( options.has("config") ) {
			parseFile((String) options.valueOf("config"), true);
		} else {
			parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			usage("Help for Maxwell:");

		if ( options.has("log_level")) {
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));
		}
		if ( options.has("host"))
			this.mysqlHost = (String) options.valueOf("host");
		if ( options.has("password"))
			this.mysqlPassword = (String) options.valueOf("password");
		if ( options.has("user"))
			this.mysqlUser = (String) options.valueOf("user");
		if ( options.has("port"))
			this.mysqlPort = Integer.valueOf((String) options.valueOf("port"));
		if ( options.has("producer"))
			this.producerType = (String) options.valueOf("producer");

		if ( options.has("kafka.bootstrap.servers"))
			this.kafkaProperties.setProperty("bootstrap.servers", (String) options.valueOf("kafka.bootstrap.servers"));

		if ( options.has("kafka_topic"))
			this.kafkaTopic = (String) options.valueOf("kafka_topic");

		if ( options.has("output_file"))
			this.outputFile = (String) options.valueOf("output_file");

		if ( options.has("max_schemas"))
			this.maxSchemas = Integer.valueOf((String)options.valueOf("max_schemas"));
		if ( options.has("include_databases"))
			this.include_databases = (String) options.valueOf("include_databases");
		
		if ( options.has("exclude_databases"))
			this.exclude_databases = (String) options.valueOf("exclude_databases");		

		if ( options.has("include_tables"))
			this.include_tables = (String) options.valueOf("include_tables");
		
		if ( options.has("exclude_tables"))
			this.exclude_tables = (String) options.valueOf("exclude_tables");
		
		
	}

	private Properties readPropertiesFile(String filename, Boolean abortOnMissing) {
		Properties p = null;
		try {
			File file = new File(filename);
			if ( !file.exists() ) {
				if ( abortOnMissing ) {
					System.err.println("Couldn't find config file: " + filename);
					System.exit(1);
				} else {
					return null;
				}
			}

			FileReader reader = new FileReader(file);
			p = new Properties();
			p.load(reader);
		} catch ( IOException e ) {
			System.err.println("Couldn't read config file: " + e);
			System.exit(1);
		}
		return p;
	}

	private void parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			return;

		this.mysqlHost     = p.getProperty("host", "127.0.0.1");
		this.mysqlPassword = p.getProperty("password");
		this.mysqlUser     = p.getProperty("user");
		this.mysqlPort     = Integer.valueOf(p.getProperty("port", "3306"));

		this.producerType    = p.getProperty("producer");
		this.outputFile      = p.getProperty("output_file");
		this.kafkaTopic      = p.getProperty("kafka_topic");
		this.include_databases   = p.getProperty("include_databases");
		this.exclude_databases   = p.getProperty("exclude_databases");
		this.include_tables   = p.getProperty("include_tables");
		this.exclude_tables   = p.getProperty("exclude_tables");

		String maxSchemaString = p.getProperty("max_schemas");
		if (maxSchemaString != null)
			this.maxSchemas      = Integer.valueOf(maxSchemaString);

		if ( p.containsKey("log_level") )
			this.log_level = parseLogLevel(p.getProperty("log_level"));

		for ( Enumeration<Object> e = p.keys(); e.hasMoreElements(); ) {
			String k = (String) e.nextElement();
			if ( k.startsWith("kafka.")) {
				this.kafkaProperties.setProperty(k.replace("kafka.", ""), p.getProperty(k));
			}
		}

	}

	private void setDefaults() {
		if ( this.producerType == null ) {
			this.producerType = "stdout";
		} else if ( this.producerType.equals("kafka")
				&& !this.kafkaProperties.containsKey("bootstrap.servers")) {
			usage("You must specify kafka.bootstrap.servers for the kafka producer!");
		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			usage("please specify --output_file=FILE to use the file producer");
		}

		if ( this.mysqlPort == null )
			this.mysqlPort = 3306;

		if ( this.mysqlHost == null ) {
			LOGGER.warn("mysql host not specified, defaulting to localhost");
			this.mysqlHost = "localhost";
		}

		if ( this.mysqlPassword == null ) {
			usage("mysql password not given!");
		}

		if ( this.maxSchemas != null )
			SchemaStore.setMaxSchemas(this.maxSchemas);
		if ( this.include_databases == null )
			this.include_databases = "";

		if ( this.exclude_databases == null )
			this.exclude_databases = "";

		if ( this.include_tables == null )
			this.include_tables = "";

		if ( this.exclude_tables == null )
			this.exclude_tables = "";

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

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
}
