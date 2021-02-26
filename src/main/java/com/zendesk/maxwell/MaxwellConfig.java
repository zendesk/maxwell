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
import com.zendesk.maxwell.util.MaxwellOptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

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
	public String deadLetterTopic;
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
	public Long pubsubRequestBytesThreshold;
	public Long pubsubMessageCountBatchSize;
	public Duration pubsubPublishDelayThreshold;
	public Duration pubsubRetryDelay;
	public Double pubsubRetryDelayMultiplier;
	public Duration pubsubMaxRetryDelay;
	public Duration pubsubInitialRpcTimeout;
	public Double pubsubRpcTimeoutMultiplier;
	public Duration pubsubMaxRpcTimeout;
	public Duration pubsubTotalTimeout;

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
	public String metricsDatadogSite;
	public String metricsDatadogHost;
	public int metricsDatadogPort;
	public Long metricsDatadogInterval;
	public boolean metricsJvm;
	public int metricsAgeSlo;

	public MaxwellDiagnosticContext.Config diagnosticConfig;

	public String clientID;
	public Long replicaServerID;

	public Position initPosition;
	public boolean replayMode;
	public boolean masterRecovery;
	public boolean ignoreProducerError;
	public boolean recaptureSchema;
	public float bufferMemoryUsage;
	public Integer maxSchemaDeltas;

	public String rabbitmqUser;
	public String rabbitmqPass;
	public String rabbitmqHost;
	public Integer rabbitmqPort;
	public String rabbitmqVirtualHost;
	public String rabbitmqURI;
	public String rabbitmqExchange;
	public String rabbitmqExchangeType;
	public boolean rabbitMqExchangeDurable;
	public boolean rabbitMqExchangeAutoDelete;
	public String rabbitmqRoutingKeyTemplate;
	public boolean rabbitmqMessagePersistent;
	public boolean rabbitmqDeclareExchange;

	public String natsUrl;
	public String natsSubject;

	public String redisHost;
	public int redisPort;
	public String redisAuth;
	public int redisDatabase;
	public String redisKey;
	public String redisStreamJsonKey;
	public String redisSentinels;
	public String redisSentinelMasterName;

	public String redisPubChannel;
	public String redisListKey;
	public String redisStreamKey;

	public String redisType;
	public String javascriptFile;
	public Scripting scripting;

	public boolean haMode;
	public String jgroupsConf;
	public String raftMemberID;

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
		this.outputConfig = new MaxwellOutputConfig();
		setup(null, null); // setup defaults
	}

	public MaxwellConfig(String argv[]) {
		this();
		this.parse(argv);
	}

	protected MaxwellOptionParser buildOptionParser() {
		final MaxwellOptionParser parser = new MaxwellOptionParser();
		parser.accepts( "config", "location of config.properties file" )
				.withRequiredArg();

		parser.separator();

		parser.accepts( "producer", "producer type: stdout|file|kafka|kinesis|nats|pubsub|sqs|rabbitmq|redis|custom" )
				.withRequiredArg();
		parser.accepts( "client_id", "unique identifier for this maxwell instance, use when running multiple maxwells" )
				.withRequiredArg();

		parser.separator();

		parser.accepts( "host", "main mysql host (contains `maxwell` database)" )
				.withRequiredArg();
		parser.accepts( "port", "port for host" )
				.withRequiredArg().ofType(Integer.class);
		parser.accepts( "user", "username for host" )
				.withRequiredArg();
		parser.accepts( "password", "password for host" )
				.withRequiredArg();

		parser.section("mysql");

		parser.accepts( "binlog_heartbeat", "enable binlog replication heartbeats, default false" )
			.withOptionalArg().ofType(Boolean.class);

		parser.accepts( "jdbc_options", "additional jdbc connection options: key1=val1&key2=val2" )
				.withRequiredArg();

		parser.accepts( "ssl", "enables SSL for all connections: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY. default: DISABLED")
				.withRequiredArg();
		parser.accepts( "replication_ssl", "overrides SSL setting for binlog connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY")
				.withRequiredArg();
		parser.accepts( "schema_ssl", "overrides SSL setting for schema capture connection: DISABLED|PREFERRED|REQUIRED|VERIFY_CA|VERIFY_IDENTITY")
				.withRequiredArg();

		parser.accepts( "schema_database", "database name for maxwell state (schema and binlog position)" )
				.withRequiredArg();
		parser.accepts( "replica_server_id", "server_id that maxwell reports to the master.  See docs for full explanation. ")
				.withRequiredArg().ofType(Long.class);

		parser.separator();

		parser.accepts( "replication_host", "mysql host to replicate from (if using separate schema-storage and replication servers)" )
				.withRequiredArg();
		parser.accepts( "replication_user", "username for replication_host" )
				.withRequiredArg();
		parser.accepts( "replication_password", "password for replication_host" )
				.withRequiredArg();
		parser.accepts( "replication_port", "port for replication_host" )
				.withRequiredArg().ofType(Integer.class);
		parser.accepts( "replication_jdbc_options", "additional jdbc connection options: key1=val1&key2=val2" )
				.withRequiredArg();

		parser.separator();

		parser.accepts( "schema_host", "host to capture schema from (use only with MaxScale replication proxy)" )
				.withRequiredArg();
		parser.accepts( "schema_user", "username for schema_host" )
				.withRequiredArg();
		parser.accepts( "schema_password", "password for schema_host" )
				.withRequiredArg();
		parser.accepts( "schema_port", "port for schema_host" )
				.withRequiredArg().ofType(Integer.class);
		parser.accepts( "schema_jdbc_options", "additional jdbc connection options: key1=val1&key2=val2" )
				.withRequiredArg();

		parser.separator();
		parser.accepts( "max_schemas", "Maximum schema-updates to keep before triggering a compaction operation.  Default: unlimited" )
				.withRequiredArg();
		parser.section("operation");

		parser.accepts( "daemon", "run maxwell in the background" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "log_level", "DEBUG|INFO|WARN|ERROR" )
				.withRequiredArg();
		parser.accepts( "env_config_prefix", "prefix of maxwell configuration environment variables, case insensitive" )
				.withRequiredArg();
		parser.separator();

		parser.accepts( "ha", "enable high-availability mode via jgroups-raft" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "jgroups_config", "location of jgroups xml configuration file" )
				.withRequiredArg();
		parser.accepts( "raft_member_id", "raft memberID.  (may also be specified in raft.xml)" )
				.withRequiredArg();

		parser.separator();

		parser.accepts( "bootstrapper", "bootstrapper type: async|sync|none. default: async" )
				.withRequiredArg();
		parser.accepts( "init_position", "initial binlog position, given as BINLOG_FILE:POSITION[:HEARTBEAT]" )
				.withRequiredArg();

		parser.accepts( "replay", "replay mode: don't persist any schema changes or binlog position updates" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "master_recovery", "enable non-GTID master position recovery code" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "gtid_mode", "enable gtid mode" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "ignore_producer_error",
				"Maxwell will be terminated on kafka/kinesis errors when false. Otherwise, those producer errors are only logged. Default to true" )
				.withOptionalArg().ofType(Boolean.class);

		parser.accepts( "recapture_schema", "recapture the latest schema.  Only use if Maxwell's schema has fallen out of sync" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "buffer_memory_usage", "Percentage of JVM memory available for transaction buffer.  Floating point between 0 and 1." )
				.withRequiredArg().ofType(Float.class);

		parser.section( "custom_producer" );
		parser.accepts( "custom_producer.factory", "fully qualified custom producer factory class" )
				.withRequiredArg();

		parser.section( "file_producer" );

		parser.accepts( "output_file", "output file for 'file' producer" )
				.withRequiredArg();

		parser.section( "kafka" );

		parser.accepts( "kafka.bootstrap.servers", "at least one kafka server, formatted as HOST:PORT[,HOST:PORT]" )
				.withRequiredArg();
		parser.accepts( "kafka_topic", "optionally provide a topic name to push to. default: maxwell" )
				.withRequiredArg();
		parser.separator();
		parser.accepts( "producer_partition_by", "database|table|primary_key|transaction_id|column|random, producer will partition by this value")
				.withRequiredArg();
		parser.accepts("producer_partition_columns", "with producer_partition_by=column, partition by the value of these columns. comma separated.")
				.withRequiredArg();
		parser.accepts( "producer_partition_by_fallback",
				"database|table|primary_key|transaction_id, fallback to this value when using 'column' partitioning and the columns are not present in the row")
				.withRequiredArg();
		parser.accepts( "producer_ack_timeout", "producer message acknowledgement timeout in milliseconds" )
				.withRequiredArg().ofType(Long.class);

		parser.separator();

		parser.accepts( "kafka_version", "kafka client library version: 0.8.2.2|0.9.0.1|0.10.0.1|0.10.2.1|0.11.0.1|1.0.0")
				.withRequiredArg();
		parser.accepts( "kafka_key_format", "how to format the kafka key; array|hash" )
				.withRequiredArg();
		parser.accepts( "kafka_partition_hash", "default|murmur3, hash function for partitioning" )
				.withRequiredArg();
		parser.accepts( "dead_letter_topic", "write to this topic when unable to publish a row for known reasons (eg message is too big)" )
				.withRequiredArg();

		parser.accepts( "kafka_partition_by", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_columns", "[deprecated]").withRequiredArg();
		parser.accepts( "kafka_partition_by_fallback", "[deprecated]").withRequiredArg();

		parser.accepts( "ddl_kafka_topic", "public DDL (schema change) events to this topic. default: kafka_topic ( see also --output_ddl )" )
				.withRequiredArg();

		parser.section( "kinesis" );
		parser.accepts( "kinesis_stream", "kinesis stream name" )
				.withOptionalArg();
		parser.accepts( "sqs_queue_uri", "SQS Queue uri" )
				.withRequiredArg();
		parser.separator();
		parser.addToSection("producer_partition_by");
		parser.addToSection("producer_partition_columns");
		parser.addToSection("producer_partition_by_fallback");
		parser.addToSection("producer_ack_timeout");

		parser.section( "nats" );

		parser.accepts( "nats_url", "Url(s) of Nats connection (comma separated). Default is localhost:4222" ).withRequiredArg();
		parser.accepts( "nats_subject", "Subject Hierarchies of Nats. Default is '%{database}.%{table}'" ).withRequiredArg();

		parser.section( "pubsub" );
		parser.accepts( "pubsub_project_id", "provide a google cloud platform project id associated with the pubsub topic" )
				.withRequiredArg();
		parser.accepts( "pubsub_topic", "pubsub topic. default: maxwell" )
				.withRequiredArg();
		parser.accepts( "ddl_pubsub_topic", "alternate pubsub topic for DDL events. default: pubsub_topic" )
				.withRequiredArg();
		parser.accepts( "pubsub_request_bytes_threshold", "threshold in bytes that triggers a batch to be sent. default: 1 byte" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_message_count_batch_size", "threshold in message count that triggers a batch to be sent. default: 1 message" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_publish_delay_threshold", "threshold in delay time (milliseconds) before batch is sent. default: 1 ms" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_retry_delay", "delay in millis before sending the first retry message. default: 100 ms" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_retry_delay_multiplier", "multiply by this ratio to increase delay time each retry. default: 1.3" )
				.withRequiredArg();
		parser.accepts( "pubsub_max_retry_delay", "maximum retry delay time in seconds. default: 60 seconds" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_initial_rpc_timeout", "timeout for initial rpc call. default: 5 seconds" )
				.withRequiredArg();
		parser.accepts( "pubsub_rpc_timeout_multiplier", "backoff delay ratio for rpc timeout. default: 1.0" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_max_rpc_timeout", "max delay in seconds for rpc timeout. default: 600 seconds" )
				.withRequiredArg().ofType(Long.class);
		parser.accepts( "pubsub_total_timeout", "maximum timeout in seconds (clamps exponential backoff)" )
				.withRequiredArg().ofType(Long.class);

		parser.section( "output" );

		parser.accepts( "output_binlog_position", "include 'position' (binlog position) field. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_gtid_position", "include 'gtid' (gtid position) field. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_commit_info", "include 'commit' and 'xid' field. default: true" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_xoffset", "include 'xoffset' (row offset inside transaction) field.  depends on '--output_commit_info'. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_nulls", "include data fields with NULL values. default: true" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_server_id", "include 'server_id' field. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_thread_id", "include 'thread_id' (client thread_id) field. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_schema_id", "include 'schema_id' (unique ID for this DDL). default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_row_query", "include 'query' field (original SQL DML query).  depends on server option 'binlog_rows_query_log_events'. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_primary_keys", "include 'primary_key' field (array of PK values). default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_primary_key_columns", "include 'primary_key_columns' field (array of PK column names). default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_null_zerodates", "convert '0000-00-00' dates/datetimes to null default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_ddl", "produce DDL records. default: false" )
				.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "output_push_timestamp", "include a microsecond timestamp representing when Maxwell sent a record. default: false" )
			.withOptionalArg().ofType(Boolean.class);
		parser.accepts( "exclude_columns", "suppress these comma-separated columns from output" )
				.withRequiredArg();
		parser.accepts("secret_key", "The secret key for the AES encryption" )
				.withRequiredArg();
		parser.accepts("encrypt", "encryption mode: [none|data|all]. default: none" )
				.withRequiredArg();

		parser.section( "filtering" );

		parser.accepts( "include_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "exclude_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "include_tables", "[deprecated]" ).withRequiredArg();
		parser.accepts( "exclude_tables", "[deprecated]" ).withRequiredArg();
		parser.accepts( "blacklist_dbs", "[deprecated]" ).withRequiredArg();
		parser.accepts( "blacklist_tables", "[deprecated]" ).withRequiredArg();

		parser.accepts( "filter", "filter specs.  specify like \"include:db.*, exclude:*.tbl, include: foo./.*bar$/, exclude:foo.bar.baz=reject\"").withRequiredArg();

		parser.accepts( "include_column_values", "[deprecated]" ).withRequiredArg();
		parser.accepts( "javascript", "file containing per-row javascript to execute" ).withRequiredArg();

		parser.section( "rabbitmq" );

		parser.accepts( "rabbitmq_user", "Username of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_pass", "Password of Rabbitmq connection. Default is guest" ).withRequiredArg();
		parser.accepts( "rabbitmq_host", "Host of Rabbitmq machine" ).withRequiredArg();
		parser.accepts( "rabbitmq_port", "Port of Rabbitmq machine" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "rabbitmq_uri", "URI to rabbit server, eg amqp://, amqps://.  other rabbitmq options take precendence over uri." ).withRequiredArg();
		parser.accepts( "rabbitmq_virtual_host", "Virtual Host of Rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange", "Name of exchange for rabbitmq publisher" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_type", "Exchange type for rabbitmq" ).withRequiredArg();
		parser.accepts( "rabbitmq_exchange_durable", "Exchange durability. Default is disabled" ).withOptionalArg();
		parser.accepts( "rabbitmq_exchange_autodelete", "If set, the exchange is deleted when all queues have finished using it. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_routing_key_template", "A string template for the routing key, '%db%' and '%table%' will be substituted. Default is '%db%.%table%'." ).withRequiredArg();
		parser.accepts( "rabbitmq_message_persistent", "Message persistence. Defaults to false" ).withOptionalArg();
		parser.accepts( "rabbitmq_declare_exchange", "Should declare the exchange for rabbitmq publisher. Defaults to true" ).withOptionalArg();

		parser.section( "redis" );

		parser.accepts( "redis_host", "Host of Redis server" ).withRequiredArg();
		parser.accepts( "redis_port", "Port of Redis server" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "redis_auth", "Authentication key for a password-protected Redis server" ).withRequiredArg();
		parser.accepts( "redis_database", "Database of Redis server" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "redis_type", "[pubsub|xadd|lpush|rpush] Selects either pubsub, xadd, lpush, or rpush. Defaults to 'pubsub'" ).withRequiredArg();
		parser.accepts( "redis_key", "Redis channel/key for Pub/Sub, XADD or LPUSH/RPUSH" ).withRequiredArg();
		parser.accepts( "redis_stream_json_key", "Redis Stream message field name for JSON message body" ).withRequiredArg();
		parser.accepts("redis_sentinels", "List of Redis sentinels in format host1:port1,host2:port2,host3:port3. It can be used instead of redis_host and redis_port" ).withRequiredArg();
		parser.accepts("redis_sentinel_master_name", "Redis sentinel master name. It is used with redis_sentinels" ).withRequiredArg();

		parser.accepts( "redis_pub_channel", "[deprecated]" ).withRequiredArg();
		parser.accepts( "redis_stream_key", "[deprecated]" ).withRequiredArg();
		parser.accepts( "redis_list_key", "[deprecated]" ).withRequiredArg();

		parser.section("metrics");

		parser.accepts( "metrics_prefix", "the prefix maxwell will apply to all metrics" ).withRequiredArg();
		parser.accepts( "metrics_type", "how maxwell metrics will be reported, at least one of slf4j|jmx|http|datadog|stackdriver" ).withRequiredArg();
		parser.accepts( "metrics_slf4j_interval", "the frequency metrics are emitted to the log, in seconds, when slf4j reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_age_slo", "the threshold in seconds for message age service level objective" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "http_port", "the port the server will bind to when http reporting is configured" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "http_path_prefix", "the http path prefix when metrics_type includes http or diagnostic is enabled, default /" ).withRequiredArg();
		parser.accepts( "http_bind_address", "the ip address the server will bind to when http reporting is configured" ).withRequiredArg();
		parser.accepts( "metrics_datadog_type", "when metrics_type includes datadog this is the way metrics will be reported, one of udp|http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_tags", "datadog tags that should be supplied, e.g. tag1:value1,tag2:value2" ).withRequiredArg();
		parser.accepts( "metrics_datadog_interval", "the frequency metrics are pushed to datadog, in seconds" ).withRequiredArg().ofType(Long.class);
		parser.accepts( "metrics_datadog_apikey", "the datadog api key to use when metrics_datadog_type = http" ).withRequiredArg();
		parser.accepts( "metrics_datadog_site", "the site to publish metrics to when metrics_datadog_type = http, one of us|eu, default us" ).withRequiredArg();
		parser.accepts( "metrics_datadog_host", "the host to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg();
		parser.accepts( "metrics_datadog_port", "the port to publish metrics to when metrics_datadog_type = udp" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "http_diagnostic", "enable http diagnostic endpoint: true|false. default: false" ).withOptionalArg().ofType(Boolean.class);
		parser.accepts( "http_diagnostic_timeout", "the http diagnostic response timeout in ms when http_diagnostic=true. default: 10000" ).withRequiredArg().ofType(Integer.class);
		parser.accepts( "metrics_jvm", "enable jvm metrics: true|false. default: false" ).withRequiredArg().ofType(Boolean.class);

		parser.accepts( "help", "display help" ).withOptionalArg().forHelp();


		return parser;
	}

	private void parse(String [] argv) {
		MaxwellOptionParser parser = buildOptionParser();
		OptionSet options = parser.parse(argv);

		Properties properties;

		if (options.has("config")) {
			properties = parseFile((String) options.valueOf("config"), true);
		} else {
			properties = parseFile(DEFAULT_CONFIG_FILE, false);
		}

		String envConfigPrefix = fetchStringOption("env_config_prefix", options, properties, null);

		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> properties.put(config.getKey().toLowerCase().replaceFirst(prefix, ""), config.getValue()));
		}

		if (options.has("help"))
			usage("Help for Maxwell:", parser, (String) options.valueOf("help"));

		setup(options, properties);

		List<?> arguments = options.nonOptionArguments();
		if(!arguments.isEmpty()) {
			usage("Unknown argument(s): " + arguments);
		}
	}

	private void setup(OptionSet options, Properties properties) {
		this.log_level = fetchStringOption("log_level", options, properties, null);

		this.maxwellMysql       = parseMysqlConfig("", options, properties);
		this.replicationMysql   = parseMysqlConfig("replication_", options, properties);
		this.schemaMysql        = parseMysqlConfig("schema_", options, properties);
		this.gtidMode           = fetchBooleanOption("gtid_mode", options, properties, System.getenv(GTID_MODE_ENV) != null);

		this.databaseName       = fetchStringOption("schema_database", options, properties, "maxwell");
		this.maxwellMysql.database = this.databaseName;

		this.producerFactory    = fetchProducerFactory(options, properties);
		this.producerType       = fetchStringOption("producer", options, properties, "stdout");
		this.producerAckTimeout = fetchLongOption("producer_ack_timeout", options, properties, 0L);
		this.bootstrapperType   = fetchStringOption("bootstrapper", options, properties, "async");
		this.clientID           = fetchStringOption("client_id", options, properties, "maxwell");
		this.replicaServerID    = fetchLongOption("replica_server_id", options, properties, 6379L);
		this.javascriptFile         = fetchStringOption("javascript", options, properties, null);

		this.kafkaTopic         	= fetchStringOption("kafka_topic", options, properties, "maxwell");
		this.deadLetterTopic        = fetchStringOption("dead_letter_topic", options, properties, null);
		this.kafkaKeyFormat     	= fetchStringOption("kafka_key_format", options, properties, "hash");
		this.kafkaPartitionKey  	= fetchStringOption("kafka_partition_by", options, properties, null);
		this.kafkaPartitionColumns  = fetchStringOption("kafka_partition_columns", options, properties, null);
		this.kafkaPartitionFallback = fetchStringOption("kafka_partition_by_fallback", options, properties, null);

		this.kafkaPartitionHash 	= fetchStringOption("kafka_partition_hash", options, properties, "default");
		this.ddlKafkaTopic 		    = fetchStringOption("ddl_kafka_topic", options, properties, this.kafkaTopic);

		this.pubsubProjectId					= fetchStringOption("pubsub_project_id", options, properties, null);
		this.pubsubTopic						= fetchStringOption("pubsub_topic", options, properties, "maxwell");
		this.ddlPubsubTopic						= fetchStringOption("ddl_pubsub_topic", options, properties, this.pubsubTopic);
		this.pubsubRequestBytesThreshold		= fetchLongOption("pubsub_request_bytes_threshold", options, properties, 1L);
		this.pubsubMessageCountBatchSize		= fetchLongOption("pubsub_message_count_batch_size", options, properties, 1L);
		this.pubsubPublishDelayThreshold		= Duration.ofMillis(fetchLongOption("pubsub_publish_delay_threshold", options, properties, 1L));
		this.pubsubRetryDelay 					= Duration.ofMillis(fetchLongOption("pubsub_retry_delay", options, properties, 100L));
		this.pubsubRetryDelayMultiplier 		= Double.parseDouble(fetchStringOption("pubsub_retry_delay_multiplier", options, properties, "1.3"));
		this.pubsubMaxRetryDelay 		 		= Duration.ofSeconds(fetchLongOption("pubsub_max_retry_delay", options, properties, 60L));
		this.pubsubInitialRpcTimeout 		 	= Duration.ofSeconds(fetchLongOption("pubsub_initial_rpc_timeout", options, properties, 5L));
		this.pubsubRpcTimeoutMultiplier 		= Double.parseDouble(fetchStringOption("pubsub_rpc_timeout_multiplier", options, properties, "1.0"));
		this.pubsubMaxRpcTimeout 		 		= Duration.ofSeconds(fetchLongOption("pubsub_max_rpc_timeout", options, properties, 600L));
		this.pubsubTotalTimeout 		 		= Duration.ofSeconds(fetchLongOption("pubsub_total_timeout", options, properties, 600L));

		this.rabbitmqHost           		= fetchStringOption("rabbitmq_host", options, properties, null);
		this.rabbitmqPort 			= fetchIntegerOption("rabbitmq_port", options, properties, null);
		this.rabbitmqUser 			= fetchStringOption("rabbitmq_user", options, properties, "guest");
		this.rabbitmqPass			= fetchStringOption("rabbitmq_pass", options, properties, "guest");
		this.rabbitmqVirtualHost    		= fetchStringOption("rabbitmq_virtual_host", options, properties, "/");
		this.rabbitmqURI 			= fetchStringOption("rabbitmq_uri", options, properties, null);
		this.rabbitmqExchange       		= fetchStringOption("rabbitmq_exchange", options, properties, "maxwell");
		this.rabbitmqExchangeType   		= fetchStringOption("rabbitmq_exchange_type", options, properties, "fanout");
		this.rabbitMqExchangeDurable 		= fetchBooleanOption("rabbitmq_exchange_durable", options, properties, false);
		this.rabbitMqExchangeAutoDelete 	= fetchBooleanOption("rabbitmq_exchange_autodelete", options, properties, false);
		this.rabbitmqRoutingKeyTemplate   	= fetchStringOption("rabbitmq_routing_key_template", options, properties, "%db%.%table%");
		this.rabbitmqMessagePersistent    	= fetchBooleanOption("rabbitmq_message_persistent", options, properties, false);
		this.rabbitmqDeclareExchange		= fetchBooleanOption("rabbitmq_declare_exchange", options, properties, true);

		this.natsUrl			= fetchStringOption("nats_url", options, properties, "nats://localhost:4222");
		this.natsSubject		= fetchStringOption("nats_subject", options, properties, "%{database}.%{table}");

		this.redisHost			= fetchStringOption("redis_host", options, properties, "localhost");
		this.redisPort			= fetchIntegerOption("redis_port", options, properties, 6379);
		this.redisAuth			= fetchStringOption("redis_auth", options, properties, null);
		this.redisDatabase		= fetchIntegerOption("redis_database", options, properties, 0);

		this.redisKey			= fetchStringOption("redis_key", options, properties, "maxwell");
		this.redisStreamJsonKey	= fetchStringOption("redis_stream_json_key", options, properties, "message");

		this.redisSentinels = fetchStringOption("redis_sentinels", options, properties, null);
		this.redisSentinelMasterName = fetchStringOption("redis_sentinel_master_name", options, properties, null);

		// deprecated options
		this.redisPubChannel = fetchStringOption("redis_pub_channel", options, properties, null);
		this.redisListKey               = fetchStringOption("redis_list_key", options, properties, null);
		this.redisStreamKey             = fetchStringOption("redis_stream_key", options, properties, null);

		this.redisType			= fetchStringOption("redis_type", options, properties, "pubsub");

		String kafkaBootstrapServers = fetchStringOption("kafka.bootstrap.servers", options, properties, null);
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

		this.producerPartitionKey = fetchStringOption("producer_partition_by", options, properties, "database");
		this.producerPartitionColumns = fetchStringOption("producer_partition_columns", options, properties, null);
		this.producerPartitionFallback = fetchStringOption("producer_partition_by_fallback", options, properties, null);

		this.kinesisStream  = fetchStringOption("kinesis_stream", options, properties, null);
		this.kinesisMd5Keys = fetchBooleanOption("kinesis_md5_keys", options, properties, false);

		this.sqsQueueUri = fetchStringOption("sqs_queue_uri", options, properties, null);

		this.outputFile = fetchStringOption("output_file", options, properties, null);

		this.metricsPrefix = fetchStringOption("metrics_prefix", options, properties, "MaxwellMetrics");
		this.metricsReportingType = fetchStringOption("metrics_type", options, properties, null);
		this.metricsSlf4jInterval = fetchLongOption("metrics_slf4j_interval", options, properties, 60L);

		this.httpPort = fetchIntegerOption("http_port", options, properties, 8080);
		this.httpBindAddress = fetchStringOption("http_bind_address", options, properties, null);
		this.httpPathPrefix = fetchStringOption("http_path_prefix", options, properties, "/");

		if (!this.httpPathPrefix.startsWith("/")) {
			this.httpPathPrefix = "/" + this.httpPathPrefix;
		}
		this.metricsDatadogType = fetchStringOption("metrics_datadog_type", options, properties, "udp");
		this.metricsDatadogTags = fetchStringOption("metrics_datadog_tags", options, properties, "");
		this.metricsDatadogAPIKey = fetchStringOption("metrics_datadog_apikey", options, properties, "");
		this.metricsDatadogSite = fetchStringOption("metrics_datadog_site", options, properties, "us");
		this.metricsDatadogHost = fetchStringOption("metrics_datadog_host", options, properties, "localhost");
		this.metricsDatadogPort = fetchIntegerOption("metrics_datadog_port", options, properties, 8125);
		this.metricsDatadogInterval = fetchLongOption("metrics_datadog_interval", options, properties, 60L);
		this.metricsJvm = fetchBooleanOption("metrics_jvm", options, properties, false);
		this.metricsAgeSlo = fetchIntegerOption("metrics_age_slo", options, properties, Integer.MAX_VALUE);

		this.diagnosticConfig = new MaxwellDiagnosticContext.Config();
		this.diagnosticConfig.enable = fetchBooleanOption("http_diagnostic", options, properties, false);
		this.diagnosticConfig.timeout = fetchLongOption("http_diagnostic_timeout", options, properties, 10000L);

		this.includeDatabases    = fetchStringOption("include_dbs", options, properties, null);
		this.excludeDatabases    = fetchStringOption("exclude_dbs", options, properties, null);
		this.includeTables       = fetchStringOption("include_tables", options, properties, null);
		this.excludeTables       = fetchStringOption("exclude_tables", options, properties, null);
		this.blacklistDatabases  = fetchStringOption("blacklist_dbs", options, properties, null);
		this.blacklistTables     = fetchStringOption("blacklist_tables", options, properties, null);
		this.filterList          = fetchStringOption("filter", options, properties, null);
		this.includeColumnValues = fetchStringOption("include_column_values", options, properties, null);

		setupInitPosition(options);

		this.replayMode =     fetchBooleanOption("replay", options, null, false);
		this.masterRecovery = fetchBooleanOption("master_recovery", options, properties, false);
		this.ignoreProducerError = fetchBooleanOption("ignore_producer_error", options, properties, true);
		this.recaptureSchema = fetchBooleanOption("recapture_schema", options, null, false);
		this.bufferMemoryUsage = fetchFloatOption("buffer_memory_usage", options, properties, 0.25f);
		this.maxSchemaDeltas = fetchIntegerOption("max_schemas", options, properties, null);

		outputConfig.includesBinlogPosition = fetchBooleanOption("output_binlog_position", options, properties, false);
		outputConfig.includesGtidPosition = fetchBooleanOption("output_gtid_position", options, properties, false);
		outputConfig.includesCommitInfo = fetchBooleanOption("output_commit_info", options, properties, true);
		outputConfig.includesXOffset = fetchBooleanOption("output_xoffset", options, properties, true);
		outputConfig.includesNulls = fetchBooleanOption("output_nulls", options, properties, true);
		outputConfig.includesServerId = fetchBooleanOption("output_server_id", options, properties, false);
		outputConfig.includesThreadId = fetchBooleanOption("output_thread_id", options, properties, false);
		outputConfig.includesSchemaId = fetchBooleanOption("output_schema_id", options, properties, false);
		outputConfig.includesRowQuery = fetchBooleanOption("output_row_query", options, properties, false);
		outputConfig.includesPrimaryKeys = fetchBooleanOption("output_primary_keys", options, properties, false);
		outputConfig.includesPrimaryKeyColumns = fetchBooleanOption("output_primary_key_columns", options, properties, false);
		outputConfig.includesPushTimestamp = fetchBooleanOption("output_push_timestamp", options, properties, false);
		outputConfig.outputDDL	= fetchBooleanOption("output_ddl", options, properties, false);
		outputConfig.zeroDatesAsNull = fetchBooleanOption("output_null_zerodates", options, properties, false);
		outputConfig.namingStrategy = fetchStringOption("output_naming_strategy", options, properties, null);
		this.excludeColumns     = fetchStringOption("exclude_columns", options, properties, null);

		setupEncryptionOptions(options, properties);

		this.haMode = fetchBooleanOption("ha", options, properties, false);
		this.jgroupsConf = fetchStringOption("jgroups_config", options, properties, "raft.xml");
		this.raftMemberID = fetchStringOption("raft_member_id", options, properties, null);
	}

	private void setupEncryptionOptions(OptionSet options, Properties properties) {
		String encryptionMode = fetchStringOption("encrypt", options, properties, "none");
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
			outputConfig.secretKey = fetchStringOption("secret_key", options, properties, null);
		}
	}

	private void setupInitPosition(OptionSet options) {
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

		String[] validPartitionBy = {"database", "table", "primary_key", "transaction_id", "thread_id", "column", "random"};
		if ( this.producerPartitionKey == null ) {
			this.producerPartitionKey = "database";
		} else if ( !ArrayUtils.contains(validPartitionBy, this.producerPartitionKey) ) {
			usageForOptions("please specify --producer_partition_by=database|table|primary_key|transaction_id|thread_id|column|random", "producer_partition_by");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionColumns) ) {
			usageForOptions("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionFallback) ) {
			usageForOptions("please specify --producer_partition_by_fallback=[database, table, primary_key, transaction_id] when using producer_partition_by=column", "producer_partition_by_fallback");
		}

	}

	private void validateFilter() {
		if ( this.filter != null )
			return;
		try {
			if ( this.filterList != null ) {
				this.filter = new Filter(this.databaseName, filterList);
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
						this.databaseName,
						includeDatabases,
						excludeDatabases,
						includeTables,
						excludeTables,
						blacklistDatabases,
						blacklistTables,
						includeColumnValues
					);
				} else {
					this.filter = new Filter(this.databaseName, "");
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
		} else if (this.producerType.equals("pubsub")) {
			if (this.pubsubRequestBytesThreshold <= 0L)
				usage("--pubsub_request_bytes_threshold must be > 0");
			if (this.pubsubMessageCountBatchSize <= 0L)
				usage("--pubsub_message_count_batch_size must be > 0");
			if (this.pubsubPublishDelayThreshold.isNegative() || this.pubsubPublishDelayThreshold.isZero())
				usage("--pubsub_publish_delay_threshold must be > 0");
			if (this.pubsubRetryDelay.isNegative() || this.pubsubRetryDelay.isZero())
				usage("--pubsub_retry_delay must be > 0");
			if (this.pubsubRetryDelayMultiplier <= 1.0)
				usage("--pubsub_retry_delay_multiplier must be > 1.0");
			if (this.pubsubMaxRetryDelay.isNegative() || this.pubsubMaxRetryDelay.isZero())
				usage("--pubsub_max_retry_delay must be > 0");
			if (this.pubsubInitialRpcTimeout.isNegative() || this.pubsubInitialRpcTimeout.isZero())
				usage("--pubsub_initial_rpc_timeout must be > 0");
			if (this.pubsubRpcTimeoutMultiplier < 1.0)
				usage("--pubsub_rpc_timeout_multiplier must be >= 1.0");
			if (this.pubsubMaxRpcTimeout.isNegative() || this.pubsubMaxRpcTimeout.isZero())
				usage("--pubsub_max_rpc_timeout must be > 0");
			if (this.pubsubTotalTimeout.isNegative() || this.pubsubTotalTimeout.isZero())
				usage("--pubsub_total_timeout must be > 0");
		} else if (this.producerType.equals("redis")) {
			if ( this.redisPubChannel != null ) {
				LOGGER.warn("--redis_pub_channel is deprecated, please use redis_key");
				this.redisKey = this.redisPubChannel;
			} else if ( this.redisListKey != null ) {
				LOGGER.warn("--redis_list_key is deprecated, please use redis_key");
				this.redisKey = this.redisListKey;
			} else if ( this.redisStreamKey != null ) {
				LOGGER.warn("--redis_stream_key is deprecated, please use redis_key");
				this.redisKey = this.redisStreamKey;
			}

			if ( this.redisKey == null ) {
				usage("please specify --redis_key=KEY");
			}

			if ((this.redisSentinelMasterName != null && this.redisSentinels == null) || (this.redisSentinels != null && this.redisSentinelMasterName == null)) {
				usageForOptions("please specify both (or none) of redis_sentinel_master_name and redis_sentinels");
			}
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
				this.maxwellMysql.sslMode,
				this.maxwellMysql.enableHeartbeat
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

		if (this.bufferMemoryUsage > 1f)
			usage("--buffer_memory_usage must be <= 1.0");

		if ( this.javascriptFile != null ) {
			try {
				this.scripting = new Scripting(this.javascriptFile);
			} catch ( Exception e ) {
				LOGGER.error("Error setting up javascript: ", e);
				System.exit(1);
			}
		}

		if ( this.maxSchemaDeltas != null ) {
			if ( this.maxSchemaDeltas <= 1 ) {
				usageForOptions("--max_schemas must a number between 1 and 2**31", "--max_schemas");
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
		String strOption = fetchStringOption(name, options, properties, null);
		if ( strOption != null ) {
			try {
				Class<?> clazz = Class.forName(strOption);
				return ProducerFactory.class.cast(clazz.newInstance());
			} catch ( ClassNotFoundException e ) {
				usageForOptions("Invalid value for " + name + ", class '" + strOption + "' not found", "--" + name);
			} catch ( IllegalAccessException | InstantiationException | ClassCastException e) {
				usageForOptions("Invalid value for " + name + ", class instantiation error", "--" + name);
			}
			return null; // unreached
		} else {
			return null;
		}
	}
}
