package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.core.producer.EncryptionMode;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

@Service
public class MaxwellConfigFactory extends AbstractConfigurationFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfigFactory.class);

	private final MaxwellCommandLineOptions maxwellCommandLineOptions;
	private final ConfigurationFileParser configurationFileParser;

	@Autowired
	public MaxwellConfigFactory(MaxwellCommandLineOptions maxwellCommandLineOptions, ConfigurationFileParser configurationFileParser) {
		this.maxwellCommandLineOptions = maxwellCommandLineOptions;
		this.configurationFileParser = configurationFileParser;
	}

	public MaxwellConfig createNewDefaultConfiguration() {
		return createFrom(null, null);
	}

	public MaxwellConfig createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(String[] args) {
		OptionSet options = maxwellCommandLineOptions.createParser().parse(args);

		Properties properties;

		if (options.has("config")) {
			properties = configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			properties = configurationFileParser.parseFile(DEFAULT_CONFIG_FILE, false);
		}

		String envConfigPrefix = fetchOption("env_config_prefix", options, properties, null);

		if (envConfigPrefix != null) {
			String prefix = envConfigPrefix.toLowerCase();
			System.getenv().entrySet().stream()
					.filter(map -> map.getKey().toLowerCase().startsWith(prefix))
					.forEach(config -> properties.put(config.getKey().toLowerCase().replaceFirst(prefix, ""), config.getValue()));
		}

		if (options.has("help"))
			throw new InvalidUsageException("Help for Maxwell:");

		MaxwellConfig config = createFrom(options, properties);

		List<?> arguments = options.nonOptionArguments();
		if (!arguments.isEmpty()) {
			throw new InvalidUsageException("Unknown argument(s): " + arguments);
		}
		return config;
	}

	private MaxwellConfig createFrom(OptionSet options, Properties properties) {
		MaxwellConfig config = new MaxwellConfig();
		config.log_level = fetchOption("log_level", options, properties, null);

		config.maxwellMysql = parseMysqlConfig("", options, properties);
		config.replicationMysql = parseMysqlConfig("replication_", options, properties);
		config.schemaMysql = parseMysqlConfig("schema_", options, properties);
		config.gtidMode = fetchBooleanOption("gtid_mode", options, properties, System.getenv(MaxwellConfig.GTID_MODE_ENV) != null);

		config.databaseName = fetchOption("schema_database", options, properties, "maxwell");
		config.maxwellMysql.database = config.databaseName;

		config.producerFactory = fetchProducerFactory(options, properties);
		config.producerType = fetchOption("producer", options, properties, "stdout");
		config.producerAckTimeout = fetchLongOption("producer_ack_timeout", options, properties, 0L);
		config.bootstrapperType = fetchOption("bootstrapper", options, properties, "async");
		config.clientID = fetchOption("client_id", options, properties, "maxwell");
		config.replicaServerID = fetchLongOption("replica_server_id", options, properties, 6379L);

		config.kafkaTopic = fetchOption("kafka_topic", options, properties, "maxwell");
		config.kafkaKeyFormat = fetchOption("kafka_key_format", options, properties, "hash");
		config.kafkaPartitionKey = fetchOption("kafka_partition_by", options, properties, null);
		config.kafkaPartitionColumns = fetchOption("kafka_partition_columns", options, properties, null);
		config.kafkaPartitionFallback = fetchOption("kafka_partition_by_fallback", options, properties, null);

		config.kafkaPartitionHash = fetchOption("kafka_partition_hash", options, properties, "default");
		config.ddlKafkaTopic = fetchOption("ddl_kafka_topic", options, properties, config.kafkaTopic);

		config.pubsubProjectId = fetchOption("pubsub_project_id", options, properties, null);
		config.pubsubTopic = fetchOption("pubsub_topic", options, properties, "maxwell");
		config.ddlPubsubTopic = fetchOption("ddl_pubsub_topic", options, properties, config.pubsubTopic);

		config.rabbitmqHost = fetchOption("rabbitmq_host", options, properties, "localhost");
		config.rabbitmqPort = Integer.parseInt(fetchOption("rabbitmq_port", options, properties, "5672"));
		config.rabbitmqUser = fetchOption("rabbitmq_user", options, properties, "guest");
		config.rabbitmqPass = fetchOption("rabbitmq_pass", options, properties, "guest");
		config.rabbitmqVirtualHost = fetchOption("rabbitmq_virtual_host", options, properties, "/");
		config.rabbitmqExchange = fetchOption("rabbitmq_exchange", options, properties, "maxwell");
		config.rabbitmqExchangeType = fetchOption("rabbitmq_exchange_type", options, properties, "fanout");
		config.rabbitMqExchangeDurable = fetchBooleanOption("rabbitmq_exchange_durable", options, properties, false);
		config.rabbitMqExchangeAutoDelete = fetchBooleanOption("rabbitmq_exchange_autodelete", options, properties, false);
		config.rabbitmqRoutingKeyTemplate = fetchOption("rabbitmq_routing_key_template", options, properties, "%db%.%table%");
		config.rabbitmqMessagePersistent = fetchBooleanOption("rabbitmq_message_persistent", options, properties, false);
		config.rabbitmqDeclareExchange = fetchBooleanOption("rabbitmq_declare_exchange", options, properties, true);

		config.redisHost = fetchOption("redis_host", options, properties, "localhost");
		config.redisPort = Integer.parseInt(fetchOption("redis_port", options, properties, "6379"));
		config.redisAuth = fetchOption("redis_auth", options, properties, null);
		config.redisDatabase = Integer.parseInt(fetchOption("redis_database", options, properties, "0"));
		config.redisPubChannel = fetchOption("redis_pub_channel", options, properties, "maxwell");
		config.redisListKey = fetchOption("redis_list_key", options, properties, "maxwell");
		config.redisType = fetchOption("redis_type", options, properties, "pubsub");

		String kafkaBootstrapServers = fetchOption("kafka.bootstrap.servers", options, properties, null);
		if (kafkaBootstrapServers != null)
			config.kafkaProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);

		if (properties != null) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("custom_producer.")) {
					config.customProducerProperties.setProperty(k.replace("custom_producer.", ""), properties.getProperty(k));
				} else if (k.startsWith("kafka.")) {
					if (k.equals("kafka.bootstrap.servers") && kafkaBootstrapServers != null)
						continue; // don't override command line bootstrap servers with config files'

					config.kafkaProperties.setProperty(k.replace("kafka.", ""), properties.getProperty(k));
				}
			}
		}

		config.producerPartitionKey = fetchOption("producer_partition_by", options, properties, "database");
		config.producerPartitionColumns = fetchOption("producer_partition_columns", options, properties, null);
		config.producerPartitionFallback = fetchOption("producer_partition_by_fallback", options, properties, null);

		config.kinesisStream = fetchOption("kinesis_stream", options, properties, null);
		config.kinesisMd5Keys = fetchBooleanOption("kinesis_md5_keys", options, properties, false);

		config.sqsQueueUri = fetchOption("sqs_queue_uri", options, properties, null);

		config.outputFile = fetchOption("output_file", options, properties, null);

		config.metricsPrefix = fetchOption("metrics_prefix", options, properties, "MaxwellMetrics");
		config.metricsReportingType = fetchOption("metrics_type", options, properties, null);
		config.metricsSlf4jInterval = fetchLongOption("metrics_slf4j_interval", options, properties, 60L);
		// TODO remove metrics_http_port support once hitting v1.11.x
		int port = Integer.parseInt(fetchOption("metrics_http_port", options, properties, "8080"));
		if (port != 8080) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			config.httpPort = port;
		} else {
			config.httpPort = Integer.parseInt(fetchOption("http_port", options, properties, "8080"));
		}
		config.httpBindAddress = fetchOption("http_bind_address", options, properties, null);
		config.httpPathPrefix = fetchOption("http_path_prefix", options, properties, "/");

		if (!config.httpPathPrefix.startsWith("/")) {
			config.httpPathPrefix = "/" + config.httpPathPrefix;
		}
		config.metricsDatadogType = fetchOption("metrics_datadog_type", options, properties, "udp");
		config.metricsDatadogTags = fetchOption("metrics_datadog_tags", options, properties, "");
		config.metricsDatadogAPIKey = fetchOption("metrics_datadog_apikey", options, properties, "");
		config.metricsDatadogHost = fetchOption("metrics_datadog_host", options, properties, "localhost");
		config.metricsDatadogPort = Integer.parseInt(fetchOption("metrics_datadog_port", options, properties, "8125"));
		config.metricsDatadogInterval = fetchLongOption("metrics_datadog_interval", options, properties, 60L);

		config.metricsJvm = fetchBooleanOption("metrics_jvm", options, properties, false);

		config.diagnosticConfig = new MaxwellDiagnosticContext.Config();
		config.diagnosticConfig.enable = fetchBooleanOption("http_diagnostic", options, properties, false);
		config.diagnosticConfig.timeout = fetchLongOption("http_diagnostic_timeout", options, properties, 10000L);

		config.includeDatabases = fetchOption("include_dbs", options, properties, null);
		config.excludeDatabases = fetchOption("exclude_dbs", options, properties, null);
		config.includeTables = fetchOption("include_tables", options, properties, null);
		config.excludeTables = fetchOption("exclude_tables", options, properties, null);
		config.blacklistDatabases = fetchOption("blacklist_dbs", options, properties, null);
		config.blacklistTables = fetchOption("blacklist_tables", options, properties, null);
		config.includeColumnValues = fetchOption("include_column_values", options, properties, null);

		if (options != null && options.has("init_position")) {
			String initPosition = (String) options.valueOf("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if (initPositionSplit.length < 2)
				throw new InvalidOptionException("Invalid init_position: " + initPosition, "--init_position");

			Long pos = 0L;
			try {
				pos = Long.valueOf(initPositionSplit[1]);
			} catch (NumberFormatException e) {
				throw new InvalidOptionException("Invalid init_position: " + initPosition, "--init_position");
			}

			Long lastHeartbeat = 0L;
			if (initPositionSplit.length > 2) {
				try {
					lastHeartbeat = Long.valueOf(initPositionSplit[2]);
				} catch (NumberFormatException e) {
					throw new InvalidOptionException("Invalid init_position: " + initPosition, "--init_position");
				}
			}

			config.initPosition = new Position(new BinlogPosition(pos, initPositionSplit[0]), lastHeartbeat);
		}

		config.replayMode = fetchBooleanOption("replay", options, null, false);
		config.masterRecovery = fetchBooleanOption("master_recovery", options, properties, false);
		config.ignoreProducerError = fetchBooleanOption("ignore_producer_error", options, properties, true);

		config.outputConfig.includesBinlogPosition = fetchBooleanOption("output_binlog_position", options, properties, false);
		config.outputConfig.includesGtidPosition = fetchBooleanOption("output_gtid_position", options, properties, false);
		config.outputConfig.includesCommitInfo = fetchBooleanOption("output_commit_info", options, properties, true);
		config.outputConfig.includesXOffset = fetchBooleanOption("output_xoffset", options, properties, true);
		config.outputConfig.includesNulls = fetchBooleanOption("output_nulls", options, properties, true);
		config.outputConfig.includesServerId = fetchBooleanOption("output_server_id", options, properties, false);
		config.outputConfig.includesThreadId = fetchBooleanOption("output_thread_id", options, properties, false);
		config.outputConfig.includesRowQuery = fetchBooleanOption("output_row_query", options, properties, false);
		config.outputConfig.outputDDL = fetchBooleanOption("output_ddl", options, properties, false);
		config.excludeColumns = fetchOption("exclude_columns", options, properties, null);

		String encryptionMode = fetchOption("encrypt", options, properties, "none");
		switch (encryptionMode) {
			case "none":
				config.outputConfig.encryptionMode = EncryptionMode.ENCRYPT_NONE;
				break;
			case "data":
				config.outputConfig.encryptionMode = EncryptionMode.ENCRYPT_DATA;
				break;
			case "all":
				config.outputConfig.encryptionMode = EncryptionMode.ENCRYPT_ALL;
				break;
			default:
				throw new InvalidUsageException("Unknown encryption mode: " + encryptionMode);
		}

		if (config.outputConfig.encryptionEnabled()) {
			config.outputConfig.secretKey = fetchOption("secret_key", options, properties, null);
		}
		return config;
	}

	private ProducerFactory fetchProducerFactory(OptionSet options, Properties properties) {
		String name = "custom_producer.factory";
		String strOption = fetchOption(name, options, properties, null);
		if (strOption != null) {
			try {
				Class<?> clazz = Class.forName(strOption);
				return ProducerFactory.class.cast(clazz.newInstance());
			} catch (ClassNotFoundException e) {
				throw new InvalidOptionException("Invalid value for " + name + ", class not found", "--" + name);
			} catch (IllegalAccessException | InstantiationException | ClassCastException e) {
				throw new InvalidOptionException("Invalid value for " + name + ", class instantiation error", "--" + name);
			}
		} else {
			return null;
		}
	}
}
