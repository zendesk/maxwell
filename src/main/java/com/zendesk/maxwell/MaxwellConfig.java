package com.zendesk.maxwell;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.FileProducer;
import com.zendesk.maxwell.producer.MaxwellKafkaProducer;

public class MaxwellConfig {
	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;

	public String currentPositionFile;

	private BinlogPosition initialPosition;
	private final Properties kafkaProperties;
	private String producerType;
	private String outputFile;

	public MaxwellConfig() {
		this.kafkaProperties = new Properties();
	}

	public Connection getMasterConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://" + mysqlHost + ":" + mysqlPort, mysqlUser, mysqlPassword);
	}

	public BinlogPosition getInitialPosition() throws FileNotFoundException, IOException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		File f = new File(this.currentPositionFile);
		if ( !f.exists() ) {
			return null;
		} else {
			Properties p = new Properties();
			p.load(new FileReader(f));

			this.initialPosition = new BinlogPosition(Integer.valueOf((String) p.get("offset")), p.getProperty("file"));
			return this.initialPosition;
		}
	}

	public void setInitialPosition(BinlogPosition position) throws IOException {
		Properties p = new Properties();
		p.setProperty("offset", String.valueOf(position.getOffset()));
		p.setProperty("file", position.getFile());

		File f = new File(this.currentPositionFile);
		FileWriter fw = new FileWriter(f);
		try {
			p.store(fw, "");
			this.initialPosition = position;
		} finally {
			fw.close();
		}
	}

	private void parseOptions(String [] argv) {
		OptionParser parser = new OptionParser();
		parser.accepts( "host" ).withRequiredArg();
		parser.accepts( "password" ).withRequiredArg();
		parser.accepts( "user" ).withRequiredArg();
		parser.accepts( "port" ).withRequiredArg();
		parser.accepts( "producer" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers" ).withRequiredArg();

		OptionSet options = parser.parse(argv);

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

		this.currentPositionFile = p.getProperty("position_file");
		this.producerType      = p.getProperty("producer");
		this.outputFile      = p.getProperty("output_file");

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
		if ( this.producerType == null )
			this.producerType = "stdout";
		if ( this.currentPositionFile == null )
			this.currentPositionFile = "maxwell.position";
		if ( this.mysqlPort == null )
			this.mysqlPort = 3306;
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}

	public AbstractProducer getProducer() throws IOException {
		switch ( this.producerType ) {
		case "file":
			return new FileProducer(this, this.outputFile);
		case "kafka":
			return new MaxwellKafkaProducer(this, this.kafkaProperties);
		case "stdout":
		default:
			return new StdoutProducer(this);
		}
	}
}
