package com.zendesk.maxwell;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.filtering.InvalidFilterException;
import com.zendesk.maxwell.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.producer.ProducerFactory;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.scripting.Scripting;
import com.zendesk.maxwell.util.AbstractConfig;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class MaxwellConfig extends AbstractConfig {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public static final String GTID_MODE_ENV = "GTID_MODE";

	public MaxwellMysqlConfig replicationMysql;
	public MaxwellMysqlConfig schemaMysql;

	public MaxwellMysqlConfig maxwellMysql;
	public Filter filter;
	public Boolean gtidMode;

	public String databaseName;

	public String includeDatabases, excludeDatabases, includeTables, excludeTables, excludeColumns, blacklistDatabases, blacklistTables, includeColumnValues;
	public String filterList;

	public ProducerFactory producerFactory; // producerFactory has precedence over producerType
	public final Properties customProducerProperties;
	public String producerType;

	public final Properties kafkaProperties;
	public String kafkaTopic;
	public String ddlKafkaTopic;
	public String kafkaKeyFormat;
	public String kafkaPartitionHash;
	public String kafkaPartitionKey;
	public String kafkaPartitionColumns;
	public String kafkaPartitionFallback;
	public String bootstrapperType;
	public int bufferedProducerSize;

	public String producerPartitionKey;
	public String producerPartitionColumns;
	public String producerPartitionFallback;

	public String kinesisStream;
	public boolean kinesisMd5Keys;

	public String sqsQueueUri;

	public String pubsubProjectId;
	public String pubsubTopic;
	public String ddlPubsubTopic;

	public Long producerAckTimeout;

	public String outputFile;
	public MaxwellOutputConfig outputConfig;
	public String log_level;

	public MetricRegistry metricRegistry;
	public HealthCheckRegistry healthCheckRegistry;

	public int httpPort;
	public String httpBindAddress;
	public String httpPathPrefix;
	public String metricsPrefix;
	public String metricsReportingType;
	public Long metricsSlf4jInterval;
	public String metricsDatadogType;
	public String metricsDatadogTags;
	public String metricsDatadogAPIKey;
	public String metricsDatadogHost;
	public int metricsDatadogPort;
	public Long metricsDatadogInterval;
	public boolean metricsJvm;

	public MaxwellDiagnosticContext.Config diagnosticConfig;

	public String clientID;
	public Long replicaServerID;

	public Position initPosition;
	public boolean replayMode;
	public boolean masterRecovery;
	public boolean ignoreProducerError;
	public boolean recaptureSchema;

	public String rabbitmqUser;
	public String rabbitmqPass;
	public String rabbitmqHost;
	public int rabbitmqPort;
	public String rabbitmqVirtualHost;
	public String rabbitmqExchange;
	public String rabbitmqExchangeType;
	public boolean rabbitMqExchangeDurable;
	public boolean rabbitMqExchangeAutoDelete;
	public String rabbitmqRoutingKeyTemplate;
	public boolean rabbitmqMessagePersistent;
	public boolean rabbitmqDeclareExchange;

	public String redisHost;
	public int redisPort;
	public String redisAuth;
	public int redisDatabase;
	public String redisPubChannel;
	public String redisListKey;
	public String redisType;
	public String javascriptFile;
	public Scripting scripting;

	public MaxwellConfig() { // argv is only null in tests
		this.customProducerProperties = new Properties();
		this.kafkaProperties = new Properties();
		this.replayMode = false;
		this.replicationMysql = new MaxwellMysqlConfig();
		this.maxwellMysql = new MaxwellMysqlConfig();
		this.schemaMysql = new MaxwellMysqlConfig();
		this.masterRecovery = false;
		this.gtidMode = false;
		this.bufferedProducerSize = 200;
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.outputConfig = new MaxwellOutputConfig();
		setup(null, null); // setup defaults
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
	}

	protected OptionParser buildOptionParser() {
		final OptionParser parser = new OptionParser();
		parser.accepts( "config", "location of config file" ).withRequiredArg();
		parser.accepts( "env_config_prefix", "prefix of env var based config, case insensitive" ).withRequiredArg();
		parser.accepts( "log_level", "log level, one of DEBUG|INFO|WARN|ERROR" ).withRequiredArg();
		parser.accepts( "daemon", "daemon, running maxwell as a daemon" ).withOptionalArg();

		parser.accepts("__separator_1");

		parser.accepts( "host", "mysql host with write access to maxwell database" ).withRequiredArg();
		parser.accepts( "port", "port for host" ).withRequiredArg();
		parser.accepts( "user", "username for host" ).withRequiredArg();
		parser.accepts( "password", "password for host" ).withRequiredArg();
		parser.accepts( "jdbc_options", "additional jdbc connection options" ).withRequiredArg();
		parser.accepts( "binlog_connector", "[deprecated]" ).withRequiredArg();

		parser.accepts( "ssl", "enables SSL for all connections: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY. default: DISABLED").withOptionalArg();
		parser.accepts( "replication_ssl", "overrides SSL setting for binlog connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY").withOptionalArg();
		parser.accepts( "schema_ssl", "overrides SSL setting for schema capture connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY").withOptionalArg();

		parser.accepts("__separator_2");

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema and replication servers)" ).withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" ).withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" ).withRequiredArg();
		parser.accepts( "replication_port", "port for replication_host" ).withRequiredArg();

		parser.accepts( "schema_host", "overrides replication_host for retrieving schema" ).withRequiredArg();
		parser.accepts( "schema_user", "username for schema_host" ).withRequiredArg();
		parser.accepts( "schema_password", "password for schema_host" ).withRequiredArg();
		parser.accepts( "schema_port", "port for schema_host" ).withRequiredArg();

		parser.accepts("__separator_3");

		parser.accepts( "producer", "producer type: stdout|file|kafka|kinesis|pubsub|sqs|rabbitmq|redis" ).withRequiredArg();
		parser.accepts( "custom_producer.factory", "fully qualified custom producer factory class" ).withRequiredArg();
		parser.accepts( "producer_ack_timeout", "producer message acknowledgement timeout" ).withRequiredArg();
		parser.accepts( "javascript", "file containing per-row javascript to execute" ).withRequiredArg();

		parser.accepts( "output_file", "output file for 'file' producer" ).withRequiredArg();

		parser.accepts( "producer_partition_by", "database|table|primary_key|column, kafka/kinesis producers will partition by this value").withRequiredArg();
		parser.accepts("producer_partition_columns",
		    "with producer_partition_by=column, partition by the value of these columns.  "
			+ "comma separated.").withRequiredArg();
		parser.accepts( "producer_partition_by_fallback", "database|table|primary_key, fallback to this value when using 'column' partitioning and the columns are not present in the row").withRequiredArg();

		parser.accepts( "kafka_version", "kafka client library version: 0.8.2.2|0.9.0.1|0.10.0.1|0.10.2.1|0.11.0.1|1.0.0").withRequiredArg();
		parser.accepts( "kafka_partition_by", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_columns", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_by_fallback", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" ).withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning" ).withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell" ).withRequiredArg();
		parser.accepts( "kafka_key_format", "how to format the kafka key; array|hash" ).withRequiredArg();

		parser.accepts( "kinesis_stream", "kinesis stream name" ).withOptionalArg();
		parser.accepts( "sqs_queue_uri", "SQS Queue uri" ).withRequiredArg();

		parser.accepts( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" ).withRequiredArg();
		parser.accepts( "pubsub_topic", "optionally provide a pubsub topic to push to. default: maxwell" ).withRequiredArg();
		parser.accepts( "ddl_pubsub_topic", "optionally provide an alternate pubsub topic to push DDL records to. default: pubsub_topic" ).withRequiredArg();

		parser.accepts("__separator_4");

		parser.accepts( "output_binlog_position", "produced records include binlog position; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_gtid_position", "produced records include gtid position; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_commit_info", "produced records include commit and xid; [true|false]. default: true" ).withOptionalArg();
		parser.accepts( "output_xoffset", "produced records include xoffset, option \"output_commit_info\" must be enabled; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_nulls", "produced records include fields with NULL values [true|false]. default: true" ).withOptionalArg();
		parser.accepts( "output_server_id", "produced records include server_id; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_thread_id", "produced records include thread_id; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_row_query", "produced records include query, binlog option \"binlog_rows_query_log_events\" must be enabled; [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "output_ddl", "produce DDL records to ddl_kafka_topic [true|false]. default: false" ).withOptionalArg();
		parser.accepts( "exclude_columns", "suppress these comma-separated columns from output" ).withRequiredArg();
		parser.accepts( "ddl_kafka_topic", "optionally provide an alternate topic to push DDL records to. default: kafka_topic" ).withRequiredArg();
		parser.accepts("secret_key", "The secret key for the AES encryption" ).withRequiredArg();
		parser.accepts("encrypt", "encryption mode: [none|data|all]. default: none" ).withRequiredArg();

		parser.accepts( "__separator_5" );

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" ).withRequiredArg();

		parser.accepts( "__separator_6" );

		parser.accepts( "replica_server_id", "server_id that maxwell reports to the master.  See docs for full explanation. ").withRequiredArg();
		parser.accepts( "client_id", "unique identifier for this maxwell replicator" ).withRequiredArg();
		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)" ).withRequiredArg();
		parser.accepts( "max_schemas", "[deprecated]" ).withRequiredArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION[:HEARTBEAT]" ).withRequiredArg();
		parser.accepts( "replay", "replay mode, don't store any information to the server" ).withOptionalArg();
		parser.accepts( "master_recovery", "(experimental) enable master position recovery code" ).withOptionalArg();
		parser.accepts( "gtid_mode", "(experimental) enable gtid mode" ).withOptionalArg();
		parser.accepts( "ignore_producer_error", "Maxwell will be terminated on kafka/kinesis errors when false. Otherwise, those producer errors are only logged. Default to true" ).withOptionalArg();
		parser.accepts( "recapture_schema", "recapture the latest schema" ).withOptionalArg();

		parser.accepts( "__separator_7" );

		parser.accepts( "include_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "exclude_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "include_tables", "[deprecated]" ).withRequiredArg();
		parser.accepts( "exclude_tables", "[deprecated]" ).withRequiredArg();
		parser.accepts( "blacklist_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "blacklist_tables", "[deprecated]" ).withRequiredArg();
		parser.accepts( "filter", "filter specs.  specify like \"include:db.*, exclude:*.tbl, include: foo./.*bar$/, exclude:foo.bar.baz=reject\"").withRequiredArg();
		parser.accepts( "include_column_values", "[deprecated]" ).withRequiredArg();

		parser.accepts( "__separator_8" );

		parser.accepts( "rabbitmq_user", "Username of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_pass", "Password of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_host", "Host of Rabbitmq machine" ).withRequiredArg();
		parser.accepts( "rabbitmq_port", "Port of Rabbitmq machine" ).withRequiredArg();
		parser.accepts( "rabbitmq_virtual_host", "Virtual Host of Rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange", "Name of exchange for rabbitmq publisher" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_type", "Exchange type for rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_durable", "Exchange durability. Default is disabled" ).withOptionalArg();
		parser.accepts( "rabbitmq_exchange_autodelete", "If set, the exchange is deleted when all queues have finished using it. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_routing_key_template", "A string template for the routing key, '%db%' and '%table%' will be substituted. Default is '%db%.%table%'." ).withRequiredArg();
		parser.accepts( "rabbitmq_message_persistent", "Message persistence. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_declare_exchange", "Should declare the exchange for rabbitmq publisher. Defaults to true" ).withOptionalArg();

		parser.accepts( "__separator_9" );

		parser.accepts( "redis_host", "Host of Redis server" ).withRequiredArg();
		parser.accepts( "redis_port", "Port of Redis server" ).withRequiredArg();
		parser.accepts( "redis_auth", "Authentication key for a password-protected Redis server" ).withRequiredArg();
		parser.accepts( "redis_database", "Database of Redis server" ).withRequiredArg();
		parser.accepts( "redis_pub_channel", "Redis Pub/Sub channel for publishing records" ).withRequiredArg();
		parser.accepts( "redis_list_key", "Redis LPUSH List Key for adding to a queue" ).withRequiredArg();
		parser.accepts( "redis_type", "[pubsub|lpush] Selects either Redis Pub/Sub or LPUSH. Defaults to 'pubsub'" ).withRequiredArg();

		parser.accepts( "__separator_10" );

		parser.accepts( "metrics_prefix", "the prefix maxwell will apply to all metrics" ).withRequiredArg();
		parser.accepts( "metrics_type", "how maxwell metrics will be reported, at least one of slf4j|jmx|http|datadog" ).withRequiredArg();
		parser.accepts( "metrics_slf4j_interval", "the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_http_port", "[deprecated]" ).withRequiredArg();
		parser.accepts( "http_port", "the port the server will bind to when http reporting is configured" ).withRequiredArg();
		parser.accepts( "http_path_prefix", "the http path prefix when metrics_type includes http or diagnostic is enabled, default /" ).withRequiredArg();
		parser.accepts( "http_bind_address", "the ip address the server will bind to when http reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_datadog_type", "when metrics_type includes datadog this is the way metrics will be reported, one of udp|http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_tags", "datadog tags that should be supplied, e.g. tag1:value1,tag2:value2" ).withRequiredArg();
		parser.accepts( "metrics_datadog_interval", "the frequency metrics are pushed to datadog, in seconds" ).withRequiredArg();
		parser.accepts( "metrics_datadog_apikey", "the datadog api key to use when metrics_datadog_type = http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_host", "the host to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg();
		parser.accepts( "metrics_datadog_port", "the port to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg();
		parser.accepts( "http_diagnostic", "enable http diagnostic endpoint: true|false. default: false" ).withOptionalArg();
		parser.accepts( "http_diagnostic_timeout", "the http diagnostic response timeout in ms when http_diagnostic=true. default: 10000" ).withRequiredArg();
		parser.accepts( "metrics_jvm", "enable jvm metrics: true|false. default: false" ).withRequiredArg();

		parser.accepts( "__separator_11" );

		parser.accepts( "help", "display help" ).forHelp();


		BuiltinHelpFormatter helpFormatter = new BuiltinHelpFormatter(200, 4) {
			@Override
			public String format(Map<String, ? extends OptionDescriptor> options) {
				this.addRows(options.values());
				String output = this.formattedHelpOutput();
				output = output.replaceAll("--__separator_.*", "");

				Pattern deprecated = Pattern.compile("^.*\\[deprecated\\].*\\n", Pattern.MULTILINE);
				return deprecated.matcher(output).replaceAll("");
			}
		};

		parser.formatHelpWith(helpFormatter);
		return parser;
	}

	private void parse(String [] argv) {
		OptionSet options = buildOptionParser().parse(argv);

		Properties properties;

		if (options.has("config")) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}

		String envConfigPrefix = fetchOption("env_config_prefix", options, properties, null);

		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> properties.put(config.getKey().toLowerCase().replaceFirst(prefix, ""), config.getValue()));
		}

		if (options.has("help"))
			usage("Help for Maxwell:");

		setup(options, properties);

		List<?> arguments = options.nonOptionArguments();
		if(!arguments.isEmpty()) {
			usage("Unknown argument(s): " + arguments);
		}
	}

	private void setup(OptionSet options, Properties properties) {
		this.log_level = fetchOption("log_level", options, properties, null);

		this.maxwellMysql       = parseMysqlConfig("", options, properties);
		this.replicationMysql   = parseMysqlConfig("replication_", options, properties);
		this.schemaMysql        = parseMysqlConfig("schema_", options, properties);
		this.gtidMode           = fetchBooleanOption("gtid_mode", options, properties, System.getenv(GTID_MODE_ENV) != null);

		this.databaseName       = fetchOption("schema_database", options, properties, "maxwell");
		this.maxwellMysql.database = this.databaseName;

		this.producerFactory    = fetchProducerFactory(options, properties);
		this.producerType       = fetchOption("producer", options, properties, "stdout");
		this.producerAckTimeout = fetchLongOption("producer_ack_timeout", options, properties, 0L);
		this.bootstrapperType   = fetchOption("bootstrapper", options, properties, "async");
		this.clientID           = fetchOption("client_id", options, properties, "maxwell");
		this.replicaServerID    = fetchLongOption("replica_server_id", options, properties, 6379L);
		this.javascriptFile         = fetchOption("javascript", options, properties, null);

		this.kafkaTopic         	= fetchOption("kafka_topic", options, properties, "maxwell");
		this.kafkaKeyFormat     	= fetchOption("kafka_key_format", options, properties, "hash");
		this.kafkaPartitionKey  	= fetchOption("kafka_partition_by", options, properties, null);
		this.kafkaPartitionColumns  = fetchOption("kafka_partition_columns", options, properties, null);
		this.kafkaPartitionFallback = fetchOption("kafka_partition_by_fallback", options, properties, null);

		this.kafkaPartitionHash 	= fetchOption("kafka_partition_hash", options, properties, "default");
		this.ddlKafkaTopic 		    = fetchOption("ddl_kafka_topic", options, properties, this.kafkaTopic);

		this.pubsubProjectId = fetchOption("pubsub_project_id", options, properties, null);
		this.pubsubTopic 		 = fetchOption("pubsub_topic", options, properties, "maxwell");
		this.ddlPubsubTopic  = fetchOption("ddl_pubsub_topic", options, properties, this.pubsubTopic);

		this.rabbitmqHost           		= fetchOption("rabbitmq_host", options, properties, "localhost");
		this.rabbitmqPort 			= Integer.parseInt(fetchOption("rabbitmq_port", options, properties, "5672"));
		this.rabbitmqUser 			= fetchOption("rabbitmq_user", options, properties, "guest");
		this.rabbitmqPass			= fetchOption("rabbitmq_pass", options, properties, "guest");
		this.rabbitmqVirtualHost    		= fetchOption("rabbitmq_virtual_host", options, properties, "/");
		this.rabbitmqExchange       		= fetchOption("rabbitmq_exchange", options, properties, "maxwell");
		this.rabbitmqExchangeType   		= fetchOption("rabbitmq_exchange_type", options, properties, "fanout");
		this.rabbitMqExchangeDurable 		= fetchBooleanOption("rabbitmq_exchange_durable", options, properties, false);
		this.rabbitMqExchangeAutoDelete 	= fetchBooleanOption("rabbitmq_exchange_autodelete", options, properties, false);
		this.rabbitmqRoutingKeyTemplate   	= fetchOption("rabbitmq_routing_key_template", options, properties, "%db%.%table%");
		this.rabbitmqMessagePersistent    	= fetchBooleanOption("rabbitmq_message_persistent", options, properties, false);
		this.rabbitmqDeclareExchange		= fetchBooleanOption("rabbitmq_declare_exchange", options, properties, true);

		this.redisHost			= fetchOption("redis_host", options, properties, "localhost");
		this.redisPort			= Integer.parseInt(fetchOption("redis_port", options, properties, "6379"));
		this.redisAuth			= fetchOption("redis_auth", options, properties, null);
		this.redisDatabase		= Integer.parseInt(fetchOption("redis_database", options, properties, "0"));
		this.redisPubChannel	= fetchOption("redis_pub_channel", options, properties, "maxwell");
		this.redisListKey		= fetchOption("redis_list_key", options, properties, "maxwell");
		this.redisType			= fetchOption("redis_type", options, properties, "pubsub");

		String kafkaBootstrapServers = fetchOption("kafka.bootstrap.servers", options, properties, null);
		if ( kafkaBootstrapServers != null )
			this.kafkaProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);

		if ( properties != null ) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("custom_producer.")) {
					this.customProducerProperties.setProperty(k.replace("custom_producer.", ""), properties.getProperty(k));
				} else if (k.startsWith("kafka.")) {
					if (k.equals("kafka.bootstrap.servers") && kafkaBootstrapServers != null)
						continue; // don't override command line bootstrap servers with config files'

					this.kafkaProperties.setProperty(k.replace("kafka.", ""), properties.getProperty(k));
				}
			}
		}

		this.producerPartitionKey = fetchOption("producer_partition_by", options, properties, "database");
		this.producerPartitionColumns = fetchOption("producer_partition_columns", options, properties, null);
		this.producerPartitionFallback = fetchOption("producer_partition_by_fallback", options, properties, null);

		this.kinesisStream  = fetchOption("kinesis_stream", options, properties, null);
		this.kinesisMd5Keys = fetchBooleanOption("kinesis_md5_keys", options, properties, false);

		this.sqsQueueUri = fetchOption("sqs_queue_uri", options, properties, null);

		this.outputFile = fetchOption("output_file", options, properties, null);

		this.metricsPrefix = fetchOption("metrics_prefix", options, properties, "MaxwellMetrics");
		this.metricsReportingType = fetchOption("metrics_type", options, properties, null);
		this.metricsSlf4jInterval = fetchLongOption("metrics_slf4j_interval", options, properties, 60L);
		// TODO remove metrics_http_port support once hitting v1.11.x
		int port = Integer.parseInt(fetchOption("metrics_http_port", options, properties, "8080"));
		if (port != 8080) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			this.httpPort = port;
		} else {
			this.httpPort = Integer.parseInt(fetchOption("http_port", options, properties, "8080"));
		}
		this.httpBindAddress = fetchOption("http_bind_address", options, properties, null);
		this.httpPathPrefix = fetchOption("http_path_prefix", options, properties, "/");

		if (!this.httpPathPrefix.startsWith("/")) {
			this.httpPathPrefix = "/" + this.httpPathPrefix;
		}
		this.metricsDatadogType = fetchOption("metrics_datadog_type", options, properties, "udp");
		this.metricsDatadogTags = fetchOption("metrics_datadog_tags", options, properties, "");
		this.metricsDatadogAPIKey = fetchOption("metrics_datadog_apikey", options, properties, "");
		this.metricsDatadogHost = fetchOption("metrics_datadog_host", options, properties, "localhost");
		this.metricsDatadogPort = Integer.parseInt(fetchOption("metrics_datadog_port", options, properties, "8125"));
		this.metricsDatadogInterval = fetchLongOption("metrics_datadog_interval", options, properties, 60L);

		this.metricsJvm = fetchBooleanOption("metrics_jvm", options, properties, false);

		this.diagnosticConfig = new MaxwellDiagnosticContext.Config();
		this.diagnosticConfig.enable = fetchBooleanOption("http_diagnostic", options, properties, false);
		this.diagnosticConfig.timeout = fetchLongOption("http_diagnostic_timeout", options, properties, 10000L);

		this.includeDatabases    = fetchOption("include_dbs", options, properties, null);
		this.excludeDatabases    = fetchOption("exclude_dbs", options, properties, null);
		this.includeTables       = fetchOption("include_tables", options, properties, null);
		this.excludeTables       = fetchOption("exclude_tables", options, properties, null);
		this.blacklistDatabases  = fetchOption("blacklist_dbs", options, properties, null);
		this.blacklistTables     = fetchOption("blacklist_tables", options, properties, null);
		this.filterList          = fetchOption("filter", options, properties, null);
		this.includeColumnValues = fetchOption("include_column_values", options, properties, null);

		if ( options != null && options.has("init_position")) {
			String initPosition = (String) options.valueOf("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if (initPositionSplit.length < 2)
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");

			Long pos = 0L;
			try {
				pos = Long.valueOf(initPositionSplit[1]);
			} catch (NumberFormatException e) {
				usageForOptions("Invalid init_position: " + initPosition, "--init_position");
			}

			Long lastHeartbeat = 0L;
			if ( initPositionSplit.length > 2 ) {
				try {
					lastHeartbeat = Long.valueOf(initPositionSplit[2]);
				} catch (NumberFormatException e) {
					usageForOptions("Invalid init_position: " + initPosition, "--init_position");
				}
			}

			this.initPosition = new Position(new BinlogPosition(pos, initPositionSplit[0]), lastHeartbeat);
		}

		this.replayMode =     fetchBooleanOption("replay", options, null, false);
		this.masterRecovery = fetchBooleanOption("master_recovery", options, properties, false);
		this.ignoreProducerError = fetchBooleanOption("ignore_producer_error", options, properties, true);
		this.recaptureSchema = fetchBooleanOption("recapture_schema", options, null, false);

		outputConfig.includesBinlogPosition = fetchBooleanOption("output_binlog_position", options, properties, false);
		outputConfig.includesGtidPosition = fetchBooleanOption("output_gtid_position", options, properties, false);
		outputConfig.includesCommitInfo = fetchBooleanOption("output_commit_info", options, properties, true);
		outputConfig.includesXOffset = fetchBooleanOption("output_xoffset", options, properties, true);
		outputConfig.includesNulls = fetchBooleanOption("output_nulls", options, properties, true);
		outputConfig.includesServerId = fetchBooleanOption("output_server_id", options, properties, false);
		outputConfig.includesThreadId = fetchBooleanOption("output_thread_id", options, properties, false);
		outputConfig.includesRowQuery = fetchBooleanOption("output_row_query", options, properties, false);
		outputConfig.outputDDL	= fetchBooleanOption("output_ddl", options, properties, false);
		this.excludeColumns     = fetchOption("exclude_columns", options, properties, null);

		String encryptionMode = fetchOption("encrypt", options, properties, "none");
		switch (encryptionMode) {
			case "none":
				outputConfig.encryptionMode = EncryptionMode.ENCRYPT_NONE;
				break;
			case "data":
				outputConfig.encryptionMode = EncryptionMode.ENCRYPT_DATA;
				break;
			case "all":
				outputConfig.encryptionMode = EncryptionMode.ENCRYPT_ALL;
				break;
			default:
				usage("Unknown encryption mode: " + encryptionMode);
				break;
		}

		if (outputConfig.encryptionEnabled()) {
			outputConfig.secretKey = fetchOption("secret_key", options, properties, null);
		}

	}

	private Properties parseFile(String filename, Boolean abortOnMissing) {
		Properties p = readPropertiesFile(filename, abortOnMissing);

		if ( p == null )
			p = new Properties();

		return p;
	}

	private void validatePartitionBy() {
		if ( this.producerPartitionKey == null && this.kafkaPartitionKey != null ) {
			LOGGER.warn("kafka_partition_by is deprecated, please use producer_partition_by");
			this.producerPartitionKey = this.kafkaPartitionKey;
		}

		if ( this.producerPartitionColumns == null && this.kafkaPartitionColumns != null) {
			LOGGER.warn("kafka_partition_columns is deprecated, please use producer_partition_columns");
			this.producerPartitionColumns = this.kafkaPartitionColumns;
		}

		if ( this.producerPartitionFallback == null && this.kafkaPartitionFallback != null ) {
			LOGGER.warn("kafka_partition_by_fallback is deprecated, please use producer_partition_by_fallback");
			this.producerPartitionFallback = this.kafkaPartitionFallback;
		}

		String[] validPartitionBy = {"database", "table", "primary_key", "column"};
		if ( this.producerPartitionKey == null ) {
			this.producerPartitionKey = "database";
		} else if ( !ArrayUtils.contains(validPartitionBy, this.producerPartitionKey) ) {
			usageForOptions("please specify --producer_partition_by=database|table|primary_key|column", "producer_partition_by");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionColumns) ) {
			usageForOptions("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionFallback) ) {
			usageForOptions("please specify --producer_partition_by_fallback=[database, table, primary_key] when using producer_partition_by=column", "producer_partition_by_fallback");
		}

	}

	private void validateFilter() {
		if ( this.filter != null )
			return;
		try {
			if ( this.filterList != null ) {
				this.filter = new Filter(filterList);
			} else {
				boolean hasOldStyleFilters =
					includeDatabases != null ||
						excludeDatabases != null ||
						includeTables != null ||
						excludeTables != null ||
						blacklistDatabases != null ||
						blacklistTables != null ||
						includeColumnValues != null;

				if ( hasOldStyleFilters ) {
					this.filter = Filter.fromOldFormat(
						includeDatabases,
						excludeDatabases,
						includeTables,
						excludeTables,
						blacklistDatabases,
						blacklistTables,
						includeColumnValues
					);
				} else {
					this.filter = new Filter();
				}
			}
		} catch (InvalidFilterException e) {
			usageForOptions("Invalid filter options: " + e.getLocalizedMessage(), "filter");
		}
	}

	public void validate() {
		validatePartitionBy();
		validateFilter();

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

			if ( !this.kafkaKeyFormat.equals("hash") && !this.kafkaKeyFormat.equals("array") )
				usageForOptions("invalid kafka_key_format: " + this.kafkaKeyFormat, "kafka_key_format");

		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			usageForOptions("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		} else if ( this.producerType.equals("kinesis") && this.kinesisStream == null) {
			usageForOptions("please specify a stream name for kinesis", "kinesis_stream");
		} else if (this.producerType.equals("sqs") && this.sqsQueueUri == null) {
			usageForOptions("please specify a queue uri for sqs", "sqs_queue_uri");
		}

		if ( !this.bootstrapperType.equals("async")
				&& !this.bootstrapperType.equals("sync")
				&& !this.bootstrapperType.equals("none") ) {
			usageForOptions("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if (this.maxwellMysql.sslMode == null) {
			this.maxwellMysql.sslMode = SSLMode.DISABLED;
		}

		if ( this.maxwellMysql.host == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.maxwellMysql.host = "localhost";
		}

		if ( this.replicationMysql.host == null
				|| this.replicationMysql.user == null ) {

			if (this.replicationMysql.host != null
					|| this.replicationMysql.user != null
					|| this.replicationMysql.password != null) {
				usageForOptions("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			this.replicationMysql = new MaxwellMysqlConfig(
				this.maxwellMysql.host,
				this.maxwellMysql.port,
				null,
				this.maxwellMysql.user,
				this.maxwellMysql.password,
				this.maxwellMysql.sslMode
			);

			this.replicationMysql.jdbcOptions = this.maxwellMysql.jdbcOptions;
		}

		if (this.replicationMysql.sslMode == null) {
			this.replicationMysql.sslMode = this.maxwellMysql.sslMode;
		}

		if (gtidMode && masterRecovery) {
			usageForOptions("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (outputConfig.includesGtidPosition && !gtidMode) {
			usageForOptions("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (this.schemaMysql.host != null) {
			if (this.schemaMysql.user == null || this.schemaMysql.password == null) {
				usageForOptions("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (this.replicationMysql.host == null) {
				usageForOptions("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (this.schemaMysql.sslMode == null) {
			this.schemaMysql.sslMode = this.maxwellMysql.sslMode;
		}


		if ( this.metricsDatadogType.contains("http") && StringUtils.isEmpty(this.metricsDatadogAPIKey) ) {
			usageForOptions("please specify metrics_datadog_apikey when metrics_datadog_type = http");
		}

		if ( this.excludeColumns != null ) {
			for ( String s : this.excludeColumns.split(",") ) {
				try {
					outputConfig.excludeColumns.add(compileStringToPattern(s));
				} catch ( InvalidFilterException e ) {
					usage("invalid exclude_columns: '" + this.excludeColumns + "': " + e.getMessage());
				}
			}
		}

		if (outputConfig.encryptionEnabled() && outputConfig.secretKey == null)
			usage("--secret_key required");

		if ( !maxwellMysql.sameServerAs(replicationMysql) && !this.bootstrapperType.equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.bootstrapperType = "none";
		}

		if ( this.javascriptFile != null ) {
			try {
				this.scripting = new Scripting(this.javascriptFile);
			} catch ( Exception e ) {
				LOGGER.error("Error setting up javascript: ", e);
				System.exit(1);
			}
		}
	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}

	public static Pattern compileStringToPattern(String name) throws InvalidFilterException {
		name = name.trim();
		if ( name.startsWith("/") ) {
			if ( !name.endsWith("/") ) {
				throw new InvalidFilterException("Invalid regular expression: " + name);
			}
			return Pattern.compile(name.substring(1, name.length() - 1));
		} else {
			return Pattern.compile("^" + Pattern.quote(name) + "$");
		}
	}

	protected ProducerFactory fetchProducerFactory(OptionSet options, Properties properties) {
		String name = "custom_producer.factory";
		String strOption = fetchOption(name, options, properties, null);
		if ( strOption != null ) {
			try {
				Class<?> clazz = Class.forName(strOption);
				return ProducerFactory.class.cast(clazz.newInstance());
			} catch ( ClassNotFoundException e ) {
				usageForOptions("Invalid value for " + name + ", class not found", "--" + name);
			} catch ( IllegalAccessException | InstantiationException | ClassCastException e) {
				usageForOptions("Invalid value for " + name + ", class instantiation error", "--" + name);
			}
			return null; // unreached
		} else {
			return null;
		}
	}
}
