package com.zendesk.maxwell;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import snaq.db.ConnectionPool;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.FileProducer;
import com.zendesk.maxwell.producer.MaxwellKafkaProducer;
import com.zendesk.maxwell.schema.SchemaPosition;

public class MaxwellConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public String  mysqlHost;
	public Integer mysqlPort;
	public String  mysqlUser;
	public String  mysqlPassword;

	private BinlogPosition initialPosition;
	private final Properties kafkaProperties;
	private String producerType;
	private String outputFile;
	private Long serverID;
	private SchemaPosition schemaPosition;

	private ConnectionPool connectionPool;

	public MaxwellConfig() {
		this.kafkaProperties = new Properties();
		this.mysqlUser = null;
		this.mysqlPassword = null;
	}

	public String getConnectionURI() {
		return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;
	}

	public ConnectionPool getConnectionPool() {
		if ( this.connectionPool != null )
			return this.connectionPool;

		this.connectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10, getConnectionURI(), mysqlUser, mysqlPassword);
		return this.connectionPool;
	}

	public void terminate() {
		this.schemaPosition.stop();
		this.schemaPosition = null;
		this.connectionPool.release();
		this.connectionPool = null;
	}

	private SchemaPosition getSchemaPosition() throws SQLException {
		if ( this.schemaPosition == null ) {
			this.schemaPosition = new SchemaPosition(this.getConnectionPool(), this.getServerID());
			this.schemaPosition.start();
		}
		return this.schemaPosition;
	}

	public BinlogPosition getInitialPosition() throws FileNotFoundException, IOException, SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = getSchemaPosition().get();
		return this.initialPosition;
	}

	public void setInitialPosition(BinlogPosition position) throws SQLException {
		this.getSchemaPosition().set(position);
	}

	private OptionParser getOptionParser() {
		OptionParser parser = new OptionParser();
		parser.accepts( "host", "mysql host" ).withRequiredArg();
		parser.accepts( "user", "mysql username" ).withRequiredArg();
		parser.accepts( "password", "mysql password" ).withRequiredArg();
		parser.accepts( "port", "mysql port" ).withRequiredArg();
		parser.accepts( "producer", "producer type: stdout|file|kafka" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
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

	public Long getServerID() throws SQLException {
		if ( this.serverID != null)
			return this.serverID;

		try ( Connection c = getConnectionPool().getConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SELECT @@server_id as server_id");
			if ( !rs.next() ) {
				throw new RuntimeException("Could not retrieve server_id!");
			}
			this.serverID = rs.getLong("server_id");
			return this.serverID;
		}
	}

}
