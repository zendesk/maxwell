package com.zendesk.maxwell;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import joptsimple.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.util.AbstractConfig;
import com.zendesk.maxwell.schema.SchemaStore;

public class MaxwellConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public MaxwellMysqlConfig replicationMysql;

	public MaxwellMysqlConfig maxwellMysql;

	public String databaseName;

	public String  includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables;

	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String producerType;
	public String kafkaPartitionHash;
	public String kafkaPartitionKey;
	public String bootstrapperType;
	public Integer bootstrapperBatchFetchSize;

	public String outputFile;
	public String log_level;

	public Integer maxSchemas;
	public BinlogPosition initPosition;
	public boolean replayMode;

	public String excludeColumns;

	public MaxwellConfig() { // argv is only null in tests
		this.kafkaProperties = new Properties();
		this.replayMode = false;
		this.replicationMysql = new MaxwellMysqlConfig();
		this.maxwellMysql = new MaxwellMysqlConfig();
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
		this.setDefaults();
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

		parser.accepts( "__separator_2" );

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" ).withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" ).withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" ).withOptionalArg();
		parser.accepts( "replication_port", "port for replicattion_host" ).withRequiredArg();

		parser.accepts( "__separator_3" );

		parser.accepts( "producer", "producer type: stdout|file|kafka" ).withRequiredArg();
		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_partition_by", "database|table|primary_key, kafka producer assigns partition by hashing the specified parameter").withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning").withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell").withOptionalArg();

		parser.accepts( "__separator_4" );

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" ).withRequiredArg();
		parser.accepts( "bootstrapper_fetch_size", "number of rows fetched at a time during bootstrapping. default: 64000" ).withRequiredArg();

		parser.accepts( "__separator_5" );

		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)").withRequiredArg();
		parser.accepts( "max_schemas", "how many old schema definitions maxwell should keep around.  default: 5").withOptionalArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION").withRequiredArg();
		parser.accepts( "replay", "replay mode, don't store any information to the server");

		parser.accepts( "__separator_6" );

		parser.accepts( "include_dbs", "include these databases, formatted as include_dbs=db1,db2").withOptionalArg();
		parser.accepts( "exclude_dbs", "exclude these databases, formatted as exclude_dbs=db1,db2").withOptionalArg();
		parser.accepts( "include_tables", "include these tables, formatted as include_tables=db1,db2").withOptionalArg();
		parser.accepts( "exclude_tables", "exclude these tables, formatted as exclude_tables=tb1,tb2").withOptionalArg();
		parser.accepts( "blacklist_dbs", "ignore data AND schema changes to these databases, formatted as blacklist_dbs=db1,db2. See the docs for details before setting this!").withOptionalArg();
		parser.accepts( "blacklist_tables", "ignore data AND schema changes to these tables, formatted as blacklist_tables=tb1,tb2. See the docs for details before setting this!").withOptionalArg();

		parser.accepts( "__separator_7" );

		parser.accepts( "help", "display help").forHelp();

		parser.accepts( "__separator_8" );

		parser.accepts( "exclude_columns", "columns which will be excluded, use format as --exclude_columns=column_name_1/column_name_2" ).withOptionalArg();

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
			usage("Help for Maxwell:");

		if ( options.has("log_level")) {
			this.log_level = parseLogLevel((String) options.valueOf("log_level"));
		}

		this.maxwellMysql.parseOptions("", options);

		this.replicationMysql.parseOptions("replication_", options);

		if ( options.has("schema_database")) {
			this.databaseName = (String) options.valueOf("schema_database");
		}

		if ( options.has("producer"))
			this.producerType = (String) options.valueOf("producer");
		if ( options.has("bootstrapper"))
			this.bootstrapperType = (String) options.valueOf("bootstrapper");
		if ( options.has("bootstrapper_fetch_size"))
			this.bootstrapperBatchFetchSize = Integer.valueOf((String) options.valueOf("bootstrapper_fetch_size"));

		if ( options.has("kafka.bootstrap.servers"))
			this.kafkaProperties.setProperty("bootstrap.servers", (String) options.valueOf("kafka.bootstrap.servers"));

		if ( options.has("kafka_topic"))
			this.kafkaTopic = (String) options.valueOf("kafka_topic");

		if ( options.has("kafka_partition_by"))
			this.kafkaPartitionKey = (String) options.valueOf("kafka_partition_by");

		if ( options.has("kafka_partition_hash"))
			this.kafkaPartitionHash = (String) options.valueOf("kafka_partition_hash");

		if ( options.has("output_file"))
			this.outputFile = (String) options.valueOf("output_file");

		if ( options.has("max_schemas"))
			this.maxSchemas = Integer.valueOf((String)options.valueOf("max_schemas"));

		if ( options.has("init_position")) {
			String initPosition = (String) options.valueOf("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if ( initPositionSplit.length != 2 )
				usage("Invalid init_position: " + initPosition);

			Long pos = 0L;
			try {
				pos = Long.valueOf(initPositionSplit[1]);
			} catch ( NumberFormatException e ) {
				usage("Invalid init_position: " + initPosition);
			}

			this.initPosition = new BinlogPosition(pos, initPositionSplit[0]);
		}

		if ( options.has("replay")) {
			this.replayMode = true;
		}

		if ( options.has("include_dbs"))
			this.includeDatabases = (String) options.valueOf("include_dbs");

		if ( options.has("exclude_dbs"))
			this.excludeDatabases = (String) options.valueOf("exclude_dbs");

		if ( options.has("include_tables"))
			this.includeTables = (String) options.valueOf("include_tables");

		if ( options.has("exclude_tables"))
			this.excludeTables = (String) options.valueOf("exclude_tables");

		if ( options.has("blacklist_dbs"))
			this.blacklistDatabases = (String) options.valueOf("blacklist_dbs");

		if ( options.has("blacklist_tables"))
			this.blacklistTables = (String) options.valueOf("blacklist_tables");

		if ( options.has("exclude_columns") ) {
			this.excludeColumns = (String) options.valueOf("exclude_columns");
		}
	}

	private void parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			return;

		this.maxwellMysql.host = p.getProperty("host", "127.0.0.1");
		this.maxwellMysql.password = p.getProperty("password");
		this.maxwellMysql.user     = p.getProperty("user");
		this.maxwellMysql.port = Integer.valueOf(p.getProperty("port", "3306"));

		this.replicationMysql.host = p.getProperty("replication_host");
		this.replicationMysql.password = p.getProperty("replication_password");
		this.replicationMysql.user      = p.getProperty("replication_user");
		this.replicationMysql.port = Integer.valueOf(p.getProperty("replication_port", "3306"));

		this.databaseName = p.getProperty("schema_database");

		this.producerType    = p.getProperty("producer");
		this.bootstrapperType = p.getProperty("bootstrapper");
		this.outputFile      = p.getProperty("output_file");
		this.kafkaTopic      = p.getProperty("kafka_topic");
		this.kafkaPartitionHash = p.getProperty("kafka_partition_hash", "default");
		this.kafkaPartitionKey = p.getProperty("kafka_partition_by", "database");
		this.includeDatabases = p.getProperty("include_dbs");
		this.excludeDatabases = p.getProperty("exclude_dbs");
		this.includeTables = p.getProperty("include_tables");
		this.excludeTables = p.getProperty("exclude_tables");
		this.blacklistDatabases = p.getProperty("blacklist_dbs");
		this.blacklistTables = p.getProperty("blacklist_tables");

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
		} else if ( this.producerType.equals("kafka") ) {
			if ( !this.kafkaProperties.containsKey("bootstrap.servers") ) {
				usage("You must specify kafka.bootstrap.servers for the kafka producer!");
			}

			if ( this.kafkaPartitionHash == null ) {
				this.kafkaPartitionHash = "default";
			} else if ( !this.kafkaPartitionHash.equals("default")
					&& !this.kafkaPartitionHash.equals("murmur3") ) {
				usage("please specify --kafka_partition_hash=default|murmur3");
			}

			if ( this.kafkaPartitionKey == null ) {
				this.kafkaPartitionKey = "database";
			} else if ( !this.kafkaPartitionKey.equals("database")
					&& !this.kafkaPartitionKey.equals("table")
					&& !this.kafkaPartitionKey.equals("primary_key") ) {
				usage("please specify --kafka_partition_by=database|table|primary_key");
			}
		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			usage("please specify --output_file=FILE to use the file producer");
		}

		if ( this.maxwellMysql.port == null )
			this.maxwellMysql.port = 3306;

		if ( this.maxwellMysql.user == null) {
			this.maxwellMysql.user = "maxwell";
		}

		if ( this.bootstrapperType == null ) {
			this.bootstrapperType = "async";
		} else if ( !this.bootstrapperType.equals("async")
				&& !this.bootstrapperType.equals("sync")
				&& !this.bootstrapperType.equals("none") ) {
			usage("please specify --bootstrapper=async|sync|none");
		}

		if ( this.bootstrapperBatchFetchSize  == null ) {
			this.bootstrapperBatchFetchSize = 64000;
		}

		if ( this.maxwellMysql.host == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.maxwellMysql.host = "localhost";
		}

		if ( this.replicationMysql.host != null && !this.bootstrapperType.equals("none") ) {
			usage("please specify --bootstrapper=none when specifying a replication host");
		}

		if ( this.replicationMysql.port == null )
			this.replicationMysql.port = 3306;

		if ( this.replicationMysql.host == null
				|| this.replicationMysql.user == null ) {

			if (this.replicationMysql.host != null
					|| this.replicationMysql.user != null
					|| this.replicationMysql.password != null) {
				usage("Specified a replication option but missing one of the following options: replication_host, replication_user, replication_password.");
			}

			this.replicationMysql = new MaxwellMysqlConfig(this.maxwellMysql.host,
									this.maxwellMysql.port,
									this.maxwellMysql.user,
									this.maxwellMysql.password);
		}

		if ( this.databaseName == null) {
			this.databaseName = "maxwell";
		}

		if ( this.maxSchemas != null )
			SchemaStore.setMaxSchemas(this.maxSchemas);
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}
}
