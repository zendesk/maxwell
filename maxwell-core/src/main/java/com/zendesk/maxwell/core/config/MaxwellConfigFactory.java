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
public class MaxwellConfigFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfigFactory.class);

	private final MaxwellCommandLineOptions maxwellCommandLineOptions;
	private final ConfigurationFileParser configurationFileParser;
	private final ConfigurationSupport configurationSupport;

	@Autowired
	public MaxwellConfigFactory(MaxwellCommandLineOptions maxwellCommandLineOptions, ConfigurationFileParser configurationFileParser, ConfigurationSupport configurationSupport) {
		this.maxwellCommandLineOptions = maxwellCommandLineOptions;
		this.configurationFileParser = configurationFileParser;
		this.configurationSupport = configurationSupport;
	}

	public MaxwellConfig createNewDefaultConfiguration() {
		return createFrom(null, null);
	}

	public MaxwellConfig createConfigurationFromArgumentsAndConfigurationFileAndEnvironmentVariables(String[] args) {
		OptionSet options = maxwellCommandLineOptions.parse(args);

		Properties properties;

		if (options.has("config")) {
			properties = configurationFileParser.parseFile((String) options.valueOf("config"), true);
		} else {
			properties = configurationFileParser.parseFile(ConfigurationSupport.DEFAULT_CONFIG_FILE, false);
		}

		String envConfigPrefix = configurationSupport.fetchOption("env_config_prefix", options, properties, null);

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
		config.setLog_level(configurationSupport.fetchOption("log_level", options, properties, null));

		config.setMaxwellMysql(configurationSupport.parseMysqlConfig("", options, properties));
		config.setReplicationMysql(configurationSupport.parseMysqlConfig("replication_", options, properties));
		config.setSchemaMysql(configurationSupport.parseMysqlConfig("schema_", options, properties));
		config.setGtidMode(configurationSupport.fetchBooleanOption("gtid_mode", options, properties, System.getenv(MaxwellConfig.GTID_MODE_ENV) != null));

		config.setDatabaseName(configurationSupport.fetchOption("schema_database", options, properties, "maxwell"));
		config.getMaxwellMysql().database = config.getDatabaseName();

		config.setProducerFactory(fetchProducerFactory(options, properties));
		config.setProducerType(configurationSupport.fetchOption("producer", options, properties, "stdout"));
		config.setProducerAckTimeout(configurationSupport.fetchLongOption("producer_ack_timeout", options, properties, 0L));
		config.setBootstrapperType(configurationSupport.fetchOption("bootstrapper", options, properties, "async"));
		config.setClientID(configurationSupport.fetchOption("client_id", options, properties, "maxwell"));
		config.setReplicaServerID(configurationSupport.fetchLongOption("replica_server_id", options, properties, 6379L));

		config.setKafkaTopic(configurationSupport.fetchOption("kafka_topic", options, properties, "maxwell"));
		config.setKafkaKeyFormat(configurationSupport.fetchOption("kafka_key_format", options, properties, "hash"));
		config.setKafkaPartitionKey(configurationSupport.fetchOption("kafka_partition_by", options, properties, null));
		config.setKafkaPartitionColumns(configurationSupport.fetchOption("kafka_partition_columns", options, properties, null));
		config.setKafkaPartitionFallback(configurationSupport.fetchOption("kafka_partition_by_fallback", options, properties, null));

		config.setKafkaPartitionHash(configurationSupport.fetchOption("kafka_partition_hash", options, properties, "default"));
		config.setDdlKafkaTopic(configurationSupport.fetchOption("ddl_kafka_topic", options, properties, config.getKafkaTopic()));

		config.setPubsubProjectId(configurationSupport.fetchOption("pubsub_project_id", options, properties, null));
		config.setPubsubTopic(configurationSupport.fetchOption("pubsub_topic", options, properties, "maxwell"));
		config.setDdlPubsubTopic(configurationSupport.fetchOption("ddl_pubsub_topic", options, properties, config.getPubsubTopic()));

		config.setRabbitmqHost(configurationSupport.fetchOption("rabbitmq_host", options, properties, "localhost"));
		config.setRabbitmqPort(Integer.parseInt(configurationSupport.fetchOption("rabbitmq_port", options, properties, "5672")));
		config.setRabbitmqUser(configurationSupport.fetchOption("rabbitmq_user", options, properties, "guest"));
		config.setRabbitmqPass(configurationSupport.fetchOption("rabbitmq_pass", options, properties, "guest"));
		config.setRabbitmqVirtualHost(configurationSupport.fetchOption("rabbitmq_virtual_host", options, properties, "/"));
		config.setRabbitmqExchange(configurationSupport.fetchOption("rabbitmq_exchange", options, properties, "maxwell"));
		config.setRabbitmqExchangeType(configurationSupport.fetchOption("rabbitmq_exchange_type", options, properties, "fanout"));
		config.setRabbitMqExchangeDurable(configurationSupport.fetchBooleanOption("rabbitmq_exchange_durable", options, properties, false));
		config.setRabbitMqExchangeAutoDelete(configurationSupport.fetchBooleanOption("rabbitmq_exchange_autodelete", options, properties, false));
		config.setRabbitmqRoutingKeyTemplate(configurationSupport.fetchOption("rabbitmq_routing_key_template", options, properties, "%db%.%table%"));
		config.setRabbitmqMessagePersistent(configurationSupport.fetchBooleanOption("rabbitmq_message_persistent", options, properties, false));
		config.setRabbitmqDeclareExchange(configurationSupport.fetchBooleanOption("rabbitmq_declare_exchange", options, properties, true));

		config.setRedisHost(configurationSupport.fetchOption("redis_host", options, properties, "localhost"));
		config.setRedisPort(Integer.parseInt(configurationSupport.fetchOption("redis_port", options, properties, "6379")));
		config.setRedisAuth(configurationSupport.fetchOption("redis_auth", options, properties, null));
		config.setRedisDatabase(Integer.parseInt(configurationSupport.fetchOption("redis_database", options, properties, "0")));
		config.setRedisPubChannel(configurationSupport.fetchOption("redis_pub_channel", options, properties, "maxwell"));
		config.setRedisListKey(configurationSupport.fetchOption("redis_list_key", options, properties, "maxwell"));
		config.setRedisType(configurationSupport.fetchOption("redis_type", options, properties, "pubsub"));

		String kafkaBootstrapServers = configurationSupport.fetchOption("kafka.bootstrap.servers", options, properties, null);
		if (kafkaBootstrapServers != null)
			config.getKafkaProperties().setProperty("bootstrap.servers", kafkaBootstrapServers);

		if (properties != null) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("custom_producer.")) {
					config.getCustomProducerProperties().setProperty(k.replace("custom_producer.", ""), properties.getProperty(k));
				} else if (k.startsWith("kafka.")) {
					if (k.equals("kafka.bootstrap.servers") && kafkaBootstrapServers != null)
						continue; // don't override command line bootstrap servers with config files'

					config.getKafkaProperties().setProperty(k.replace("kafka.", ""), properties.getProperty(k));
				}
			}
		}

		config.setProducerPartitionKey(configurationSupport.fetchOption("producer_partition_by", options, properties, "database"));
		config.setProducerPartitionColumns(configurationSupport.fetchOption("producer_partition_columns", options, properties, null));
		config.setProducerPartitionFallback(configurationSupport.fetchOption("producer_partition_by_fallback", options, properties, null));

		config.setKinesisStream(configurationSupport.fetchOption("kinesis_stream", options, properties, null));
		config.setKinesisMd5Keys(configurationSupport.fetchBooleanOption("kinesis_md5_keys", options, properties, false));

		config.setSqsQueueUri(configurationSupport.fetchOption("sqs_queue_uri", options, properties, null));

		config.setOutputFile(configurationSupport.fetchOption("output_file", options, properties, null));

		config.setMetricsPrefix(configurationSupport.fetchOption("metrics_prefix", options, properties, "MaxwellMetrics"));
		config.setMetricsReportingType(configurationSupport.fetchOption("metrics_type", options, properties, null));
		config.setMetricsSlf4jInterval(configurationSupport.fetchLongOption("metrics_slf4j_interval", options, properties, 60L));
		// TODO remove metrics_http_port support once hitting v1.11.x
		int port = Integer.parseInt(configurationSupport.fetchOption("metrics_http_port", options, properties, "8080"));
		if (port != 8080) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			config.setHttpPort(port);
		} else {
			config.setHttpPort(Integer.parseInt(configurationSupport.fetchOption("http_port", options, properties, "8080")));
		}
		config.setHttpBindAddress(configurationSupport.fetchOption("http_bind_address", options, properties, null));
		config.setHttpPathPrefix(configurationSupport.fetchOption("http_path_prefix", options, properties, "/"));

		if (!config.getHttpPathPrefix().startsWith("/")) {
			config.setHttpPathPrefix("/" + config.getHttpPathPrefix());
		}
		config.setMetricsDatadogType(configurationSupport.fetchOption("metrics_datadog_type", options, properties, "udp"));
		config.setMetricsDatadogTags(configurationSupport.fetchOption("metrics_datadog_tags", options, properties, ""));
		config.setMetricsDatadogAPIKey(configurationSupport.fetchOption("metrics_datadog_apikey", options, properties, ""));
		config.setMetricsDatadogHost(configurationSupport.fetchOption("metrics_datadog_host", options, properties, "localhost"));
		config.setMetricsDatadogPort(Integer.parseInt(configurationSupport.fetchOption("metrics_datadog_port", options, properties, "8125")));
		config.setMetricsDatadogInterval(configurationSupport.fetchLongOption("metrics_datadog_interval", options, properties, 60L));

		config.setMetricsJvm(configurationSupport.fetchBooleanOption("metrics_jvm", options, properties, false));

		config.setDiagnosticConfig(new MaxwellDiagnosticContext.Config());
		config.getDiagnosticConfig().enable = configurationSupport.fetchBooleanOption("http_diagnostic", options, properties, false);
		config.getDiagnosticConfig().timeout = configurationSupport.fetchLongOption("http_diagnostic_timeout", options, properties, 10000L);

		config.setIncludeDatabases(configurationSupport.fetchOption("include_dbs", options, properties, null));
		config.setExcludeDatabases(configurationSupport.fetchOption("exclude_dbs", options, properties, null));
		config.setIncludeTables(configurationSupport.fetchOption("include_tables", options, properties, null));
		config.setExcludeTables(configurationSupport.fetchOption("exclude_tables", options, properties, null));
		config.setBlacklistDatabases(configurationSupport.fetchOption("blacklist_dbs", options, properties, null));
		config.setBlacklistTables(configurationSupport.fetchOption("blacklist_tables", options, properties, null));
		config.setIncludeColumnValues(configurationSupport.fetchOption("include_column_values", options, properties, null));

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

			config.setInitPosition(new Position(new BinlogPosition(pos, initPositionSplit[0]), lastHeartbeat));
		}

		config.setReplayMode(configurationSupport.fetchBooleanOption("replay", options, null, false));
		config.setMasterRecovery(configurationSupport.fetchBooleanOption("master_recovery", options, properties, false));
		config.setIgnoreProducerError(configurationSupport.fetchBooleanOption("ignore_producer_error", options, properties, true));

		config.getOutputConfig().includesBinlogPosition = configurationSupport.fetchBooleanOption("output_binlog_position", options, properties, false);
		config.getOutputConfig().includesGtidPosition = configurationSupport.fetchBooleanOption("output_gtid_position", options, properties, false);
		config.getOutputConfig().includesCommitInfo = configurationSupport.fetchBooleanOption("output_commit_info", options, properties, true);
		config.getOutputConfig().includesXOffset = configurationSupport.fetchBooleanOption("output_xoffset", options, properties, true);
		config.getOutputConfig().includesNulls = configurationSupport.fetchBooleanOption("output_nulls", options, properties, true);
		config.getOutputConfig().includesServerId = configurationSupport.fetchBooleanOption("output_server_id", options, properties, false);
		config.getOutputConfig().includesThreadId = configurationSupport.fetchBooleanOption("output_thread_id", options, properties, false);
		config.getOutputConfig().includesRowQuery = configurationSupport.fetchBooleanOption("output_row_query", options, properties, false);
		config.getOutputConfig().outputDDL = configurationSupport.fetchBooleanOption("output_ddl", options, properties, false);
		config.setExcludeColumns(configurationSupport.fetchOption("exclude_columns", options, properties, null));

		String encryptionMode = configurationSupport.fetchOption("encrypt", options, properties, "none");
		switch (encryptionMode) {
			case "none":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_NONE;
				break;
			case "data":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_DATA;
				break;
			case "all":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_ALL;
				break;
			default:
				throw new InvalidUsageException("Unknown encryption mode: " + encryptionMode);
		}

		if (config.getOutputConfig().encryptionEnabled()) {
			config.getOutputConfig().secretKey = configurationSupport.fetchOption("secret_key", options, properties, null);
		}
		return config;
	}

	private ProducerFactory fetchProducerFactory(OptionSet options, Properties properties) {
		String name = "custom_producer.factory";
		String strOption = configurationSupport.fetchOption(name, options, properties, null);
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
