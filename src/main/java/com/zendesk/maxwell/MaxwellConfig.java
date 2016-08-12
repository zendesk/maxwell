package com.zendesk.maxwell;

import java.util.*;

import joptsimple.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.I0Itec.zkclient.ZkClient;
import kafka.utils.ZKStringSerializer$;

import com.zendesk.maxwell.util.AbstractConfig;

public class MaxwellConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public MaxwellMysqlConfig replicationMysql;

	public MaxwellMysqlConfig maxwellMysql;
	public MaxwellFilter filter;

	public String databaseName;

	public String  includeDatabases, excludeDatabases, includeTables, excludeTables, excludeColumns, blacklistDatabases, blacklistTables;

	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String kafkaKeyFormat;
	public String producerType;
	public String kafkaPartitionHash;
	public String kafkaPartitionKey;
	public String bootstrapperType;
    public String kafkaTopicPerTable;
    public Integer zkConnectionTimeout;
    public Integer zkSessionTimeout;
    public String zkConnection;
    public ZkClient zkClient;

	public String outputFile;
	public String log_level;

	public BinlogPosition initPosition;
	public boolean replayMode;

	public MaxwellConfig() { // argv is only null in tests
		this.kafkaProperties = new Properties();
		this.replayMode = false;
		this.replicationMysql = new MaxwellMysqlConfig();
		this.maxwellMysql = new MaxwellMysqlConfig();
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
		this.validate();
	}

	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();

		parser.accepts( "__separator_1" );

		parser.accepts( "host", "mysql host with write access to maxwell database" ).withRequiredArg();
		parser.accepts( "port", "port for host" ).withRequiredArg();
		parser.accepts( "user", "username for host" ).withRequiredArg();
		parser.accepts( "password", "password for host" ).withOptionalArg();
		parser.accepts( "jdbc_options", "additional jdbc connection options" ).withOptionalArg();

		parser.accepts( "__separator_2" );

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" ).withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" ).withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" ).withOptionalArg();
		parser.accepts( "replication_port", "port for replication_host" ).withRequiredArg();

		parser.accepts( "__separator_3" );

		parser.accepts( "producer", "producer type: stdout|file|kafka" ).withRequiredArg();
		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_partition_by", "database|table|primary_key, kafka producer assigns partition by hashing the specified parameter").withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning").withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withOptionalArg();
		parser.accepts( "kafka_key_format", "how to format the kafka key; array|hash").withOptionalArg();
        //kafka_topic_per_table string to have the format for the kafka_topic
        parser.accepts( "kafka_topic_per_table", "how to format the kafka topic string for the tables, example: 'namespace_%{db}_%{table}' where db and table are replaced with the names depending on the row").withOptionalArg();
        parser.accepts( "zk_connection_timeout_ms", "Used to check if kafka topic exists").withOptionalArg();
        parser.accepts( "zk_session_timeout_ms", "Used to check if kafka topic exists").withOptionalArg();
        parser.accepts( "zk_connection", "Zookeeper to check if the kafka topic exists").withOptionalArg();

		parser.accepts( "__separator_4" );

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" ).withRequiredArg();

		parser.accepts( "__separator_5" );

		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)").withRequiredArg();
		parser.accepts( "max_schemas", "deprecated.").withOptionalArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION").withRequiredArg();
		parser.accepts( "replay", "replay mode, don't store any information to the server");

		parser.accepts( "__separator_6" );

		parser.accepts( "include_dbs", "include these databases, formatted as include_dbs=db1,db2").withOptionalArg();
		parser.accepts( "exclude_dbs", "exclude these databases, formatted as exclude_dbs=db1,db2").withOptionalArg();
		parser.accepts( "include_tables", "include these tables, formatted as include_tables=db1,db2").withOptionalArg();
		parser.accepts( "exclude_tables", "exclude these tables, formatted as exclude_tables=tb1,tb2").withOptionalArg();
		parser.accepts( "exclude_columns", "exclude these columns, formatted as exclude_columns=col1,col2" ).withOptionalArg();
		parser.accepts( "blacklist_dbs", "ignore data AND schema changes to these databases, formatted as blacklist_dbs=db1,db2. See the docs for details before setting this!").withOptionalArg();
		parser.accepts( "blacklist_tables", "ignore data AND schema changes to these tables, formatted as blacklist_tables=tb1,tb2. See the docs for details before setting this!").withOptionalArg();

		parser.accepts( "__separator_7" );

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
			usageForOptions("unknown log level: " + level, "--log_level");
		return level;
	}


	private String fetchOption(String name, OptionSet options, Properties properties, String defaultVal) {
		if ( options.has(name) )
			return (String) options.valueOf(name);
		else if ( (properties != null) && properties.containsKey(name) )
			return (String) properties.getProperty(name);
		else
			return defaultVal;
	}

	private MaxwellMysqlConfig parseMysqlConfig(String prefix, OptionSet options, Properties properties) {
		MaxwellMysqlConfig config = new MaxwellMysqlConfig();
		config.host     = fetchOption(prefix + "host", options, properties, null);
		config.password = fetchOption(prefix + "password", options, properties, null);
		config.user     = fetchOption(prefix + "user", options, properties, null);
		config.port     = Integer.valueOf(fetchOption(prefix + "port", options, properties, "3306"));
		config.setJDBCOptions(fetchOption(prefix + "jdbc_options", options, properties, null));
		return config;
	}

	private void parse(String [] argv) {
		OptionSet options = buildOptionParser().parse(argv);

		Properties properties;

		if ( options.has("config") ) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}

		if ( options.has("help") )
			usage("Help for Maxwell:");

		this.log_level = fetchOption("log_level", options, properties, null);

		this.maxwellMysql       = parseMysqlConfig("", options, properties);
		this.replicationMysql   = parseMysqlConfig("replication_", options, properties);

		this.databaseName       = fetchOption("schema_database", options, properties, "maxwell");
		this.producerType       = fetchOption("producer", options, properties, "stdout");
		this.bootstrapperType   = fetchOption("bootstrapper", options, properties, "async");

		this.kafkaTopic         = fetchOption("kafka_topic", options, properties, "maxwell");
		this.kafkaKeyFormat     = fetchOption("kafka_key_format", options, properties, "hash");
		this.kafkaPartitionKey  = fetchOption("kafka_partition_by", options, properties, "database");
		this.kafkaPartitionHash = fetchOption("kafka_partition_hash", options, properties, "default");
        this.kafkaTopicPerTable = fetchOption("kafka_topic_per_table", options, properties, "maxwell");
        this.zkConnectionTimeout= Integer.parseInt(fetchOption("zk_connection_timeout_ms", options, properties, "default"));
        this.zkSessionTimeout   = Integer.parseInt(fetchOption("zk_session_timeout_ms", options, properties, "default"));
        this.zkConnection       = fetchOption("zk_connection", options, properties, "default");
        this.zkClient           = new ZkClient(this.zkConnection, this.zkSessionTimeout, this.zkConnectionTimeout, ZKStringSerializer$.MODULE$);


		String kafkaBootstrapServers = fetchOption("kafka.bootstrap.servers", options, properties, null);
		if ( kafkaBootstrapServers != null )
			this.kafkaProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);

		for ( Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
			String k = (String) e.nextElement();
			if ( k.startsWith("kafka.")) {
				this.kafkaProperties.setProperty(k.replace("kafka.", ""), properties.getProperty(k));
			}
		}

		this.outputFile         = fetchOption("output_file", options, properties, null);

		this.includeDatabases   = fetchOption("include_dbs", options, properties, null);
		this.excludeDatabases   = fetchOption("exclude_dbs", options, properties, null);
		this.includeTables      = fetchOption("include_tables", options, properties, null);
		this.excludeTables      = fetchOption("exclude_tables", options, properties, null);
		this.excludeColumns     = fetchOption("exclude_columns", options, properties, null);
		this.blacklistDatabases = fetchOption("blacklist_dbs", options, properties, null);
		this.blacklistTables    = fetchOption("blacklist_tables", options, properties, null);

		if ( options.has("init_position")) {
			String initPosition = (String) options.valueOf("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if ( initPositionSplit.length != 2 )
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");

			Long pos = 0L;
			try {
				pos = Long.valueOf(initPositionSplit[1]);
			} catch ( NumberFormatException e ) {
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");
			}

			this.initPosition = new BinlogPosition(pos, initPositionSplit[0]);
		}

		if ( options.has("replay")) {
			this.replayMode = true;
		}
	}

	private Properties parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			p = new Properties();
		return p;
	}

	private void validate() {
		if ( this.producerType.equals("kafka") ) {
			if ( !this.kafkaProperties.containsKey("bootstrap.servers") ) {
				usageForOptions("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
			}

			if ( this.kafkaPartitionHash == null ) {
				this.kafkaPartitionHash = "default";
			} else if ( !this.kafkaPartitionHash.equals("default")
					&& !this.kafkaPartitionHash.equals("murmur3") ) {
				usageForOptions("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
			}

			if ( this.kafkaPartitionKey == null ) {
				this.kafkaPartitionKey = "database";
			} else if ( !this.kafkaPartitionKey.equals("database")
					&& !this.kafkaPartitionKey.equals("table")
					&& !this.kafkaPartitionKey.equals("primary_key") ) {
				usageForOptions("please specify --kafka_partition_by=database|table|primary_key", "kafka_partition_by");
			}


			if ( !this.kafkaKeyFormat.equals("hash") && !this.kafkaKeyFormat.equals("array") )
				usageForOptions("invalid kafka_key_format: " + this.kafkaKeyFormat, "kafka_key_format");

		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			usageForOptions("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		}

		if ( !this.bootstrapperType.equals("async")
				&& !this.bootstrapperType.equals("sync")
				&& !this.bootstrapperType.equals("none") ) {
			usageForOptions("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if ( this.maxwellMysql.host == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.maxwellMysql.host = "localhost";
		}

		if ( this.replicationMysql.host != null && !this.bootstrapperType.equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.bootstrapperType = "none";
		}

		if ( this.replicationMysql.host == null
				|| this.replicationMysql.user == null ) {

			if (this.replicationMysql.host != null
					|| this.replicationMysql.user != null
					|| this.replicationMysql.password != null) {
				usageForOptions("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			this.replicationMysql = new MaxwellMysqlConfig(this.maxwellMysql.host,
									this.maxwellMysql.port,
									this.maxwellMysql.user,
									this.maxwellMysql.password);

			this.replicationMysql.jdbcOptions = this.maxwellMysql.jdbcOptions;
		}

		try {
			this.filter = new MaxwellFilter(
					includeDatabases,
					excludeDatabases,
					includeTables,
					excludeTables,
					blacklistDatabases,
					blacklistTables,
					excludeColumns
			);
		} catch (MaxwellInvalidFilterException e) {
			usage("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
}
