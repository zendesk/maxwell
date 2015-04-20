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

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.FileProducer;
import com.zendesk.maxwell.producer.MaxwellKafkaProducer;

public class MaxwellConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;

	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String producerType;
	public String outputFile;


	public MaxwellConfig() {
		this.kafkaProperties = new Properties();
		this.mysqlUser = null;
		this.mysqlPassword = null;
	}

	public String getConnectionURI() {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
	}


	private OptionParser getOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "host", "mysql host" ).withRequiredArg();
		parser.accepts( "user", "mysql username" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port" ).withRequiredArg();
		parser.accepts( "producer", "producer type: stdout|file|kafka" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withOptionalArg();
		parser.accepts( "help", "display help").forHelp();
		parser.formatHelpWith(new BuiltinHelpFormatter(160, 4));
		return parser;
	}

	private void parseOptions(String [] argv) {
		OptionSet options = getOptionParser().parse(argv);

		if ( options.has("help") )
			usage("Help for Maxwell:");

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
	}

	private void parseFile(String filename) throws IOException {
		Properties p = new Properties();
		File file = new File(filename);
		if ( !file.exists() )
			return;

		FileReader reader = new FileReader(file);
		p.load(reader);

		this.mysqlHost     = p.getProperty("host", "127.0.0.1");
		this.mysqlPassword = p.getProperty("password");
		this.mysqlUser     = p.getProperty("user");
		this.mysqlPort     = Integer.valueOf(p.getProperty("port", "3306"));

		this.producerType    = p.getProperty("producer");
		this.outputFile      = p.getProperty("output_file");
		this.kafkaTopic      = p.getProperty("kafka_topic");

		for ( Enumeration<Object> e = p.keys(); e.hasMoreElements(); ) {
			String k = (String) e.nextElement();
			if ( k.startsWith("kafka.")) {
				this.kafkaProperties.setProperty(k.replace("kafka.", ""), p.getProperty(k));
			}
		}

	}

	public static MaxwellConfig buildConfig(String filename, String [] argv) throws IOException {
		MaxwellConfig config = new MaxwellConfig();

		config.parseFile(filename);
		config.parseOptions(argv);
		config.setDefaults();

		return config;
	}

	private void setDefaults() {
		if ( this.producerType == null ) {
			this.producerType = "stdout";
		} else if ( this.producerType.equals("kafka")
				&& !this.kafkaProperties.containsKey("bootstrap.servers")) {
			usage("You must specify kafka.bootstrap.servers for the kafka producer!");
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

	}

	private void usage(String string) {
		System.out.println(string);
		System.out.println();
		try {
			getOptionParser().printHelpOn(System.out);
			System.exit(1);
		} catch (IOException e) {
		}
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
}
