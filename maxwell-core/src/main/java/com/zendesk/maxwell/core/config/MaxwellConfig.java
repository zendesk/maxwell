package com.zendesk.maxwell.core.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.core.producer.EncryptionMode;
import com.zendesk.maxwell.core.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.replication.Position;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.regex.Pattern;

public class MaxwellConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

	public static MaxwellConfig newDefault() {
		MaxwellConfig config = new MaxwellConfig();
		config.replayMode = false;
		config.replicationMysql = new MaxwellMysqlConfig();
		config.maxwellMysql = new MaxwellMysqlConfig();
		config.schemaMysql = new MaxwellMysqlConfig();
		config.masterRecovery = false;
		config.gtidMode = false;
		config.bufferedProducerSize = 200;
		config.metricRegistry = new MetricRegistry();
		config.healthCheckRegistry = new HealthCheckRegistry();
		config.outputConfig = new MaxwellOutputConfig();

		config.log_level = null;

		config.gtidMode           = System.getenv(GTID_MODE_ENV) != null;

		config.databaseName       = "maxwell";
		config.maxwellMysql.database = config.databaseName;

		config.producerType       = "stdout";
		config.producerAckTimeout = 0L;
		config.bootstrapperType   = "async";
		config.clientID           = "maxwell";
		config.replicaServerID    = 6379L;

		config.kafkaTopic         	= "maxwell";
		config.kafkaKeyFormat     	= "hash";
		config.kafkaPartitionKey  	= null;
		config.kafkaPartitionColumns  = null;
		config.kafkaPartitionFallback = null;

		config.kafkaPartitionHash 	= "default";
		config.ddlKafkaTopic 		    = config.kafkaTopic;

		config.pubsubProjectId = null;
		config.pubsubTopic 		 = "maxwell";
		config.ddlPubsubTopic  = config.pubsubTopic;

		config.rabbitmqHost           		= "localhost";
		config.rabbitmqPort 			= 5672;
		config.rabbitmqUser 			= "guest";
		config.rabbitmqPass			= "guest";
		config.rabbitmqVirtualHost    		= "/";
		config.rabbitmqExchange       		= "maxwell";
		config.rabbitmqExchangeType   		= "fanout";
		config.rabbitMqExchangeDurable 		= false;
		config.rabbitMqExchangeAutoDelete 	= false;
		config.rabbitmqRoutingKeyTemplate   	= "%db%.%table%";
		config.rabbitmqMessagePersistent    	= false;
		config.rabbitmqDeclareExchange		= true;

		config.redisHost			= "localhost";
		config.redisPort			= 6379;
		config.redisAuth			= null;
		config.redisDatabase		= 0;
		config.redisPubChannel	= "maxwell";
		config.redisListKey		= "maxwell";
		config.redisType			= "pubsub";


		config.producerPartitionKey = "database";
		config.producerPartitionColumns = null;
		config.producerPartitionFallback = null;

		config.kinesisStream  = null;
		config.kinesisMd5Keys = false;

		config.sqsQueueUri = null;

		config.outputFile = null;

		config.metricsPrefix = "MaxwellMetrics";
		config.metricsReportingType = null;
		config.metricsSlf4jInterval = 60L;
		// TODO remove metrics_http_port support once hitting v1.11.x
		config.httpPort = 8080;
		config.httpBindAddress = null;
		config.httpPathPrefix = "/";
		config.metricsDatadogType = "udp";
		config.metricsDatadogTags = "";
		config.metricsDatadogAPIKey = "";
		config.metricsDatadogHost = "localhost";
		config.metricsDatadogPort = 8125;
		config.metricsDatadogInterval = 60L;

		config.metricsJvm = false;

		config.diagnosticConfig = new MaxwellDiagnosticContext.Config();
		config.diagnosticConfig.enable = false;
		config.diagnosticConfig.timeout = 10000L;

		config.includeDatabases    = null;
		config.excludeDatabases    = null;
		config.includeTables       = null;
		config.excludeTables       = null;
		config.blacklistDatabases  = null;
		config.blacklistTables     = null;
		config.includeColumnValues = null;

		config.replayMode =     false;
		config.masterRecovery = false;
		config.ignoreProducerError = true;

		config.outputConfig.includesBinlogPosition = false;
		config.outputConfig.includesGtidPosition = false;
		config.outputConfig.includesCommitInfo = true;
		config.outputConfig.includesXOffset = true;
		config.outputConfig.includesNulls = true;
		config.outputConfig.includesServerId = false;
		config.outputConfig.includesThreadId = false;
		config.outputConfig.includesRowQuery = false;
		config.outputConfig.outputDDL	= false;
		config.excludeColumns     = null;

		config.outputConfig.encryptionMode = EncryptionMode.ENCRYPT_NONE;
		return config;
	}

	public static final String GTID_MODE_ENV = "GTID_MODE";

	public MaxwellMysqlConfig replicationMysql;
	public MaxwellMysqlConfig schemaMysql;

	public MaxwellMysqlConfig maxwellMysql;
	public MaxwellFilter filter;
	public Boolean gtidMode;

	public String databaseName;

	public String includeDatabases;
	public String excludeDatabases;
	public String includeTables;
	public String excludeTables;
	public String excludeColumns;
	public String blacklistDatabases;
	public String blacklistTables;
	public String includeColumnValues;

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

	private MaxwellConfig() {
		this.customProducerProperties = new Properties();
		this.kafkaProperties = new Properties();
	}

	public void validate() {
		validatePartitionBy();

		if ( this.producerType.equals("kafka") ) {
			if ( !this.kafkaProperties.containsKey("bootstrap.servers") ) {
				throw new InvalidOptionException("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
			}

			if ( this.kafkaPartitionHash == null ) {
				this.kafkaPartitionHash = "default";
			} else if ( !this.kafkaPartitionHash.equals("default")
					&& !this.kafkaPartitionHash.equals("murmur3") ) {
				throw new InvalidOptionException("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
			}

			if ( !this.kafkaKeyFormat.equals("hash") && !this.kafkaKeyFormat.equals("array") )
				throw new InvalidOptionException("invalid kafka_key_format: " + this.kafkaKeyFormat, "kafka_key_format");

		} else if ( this.producerType.equals("file")
				&& this.outputFile == null) {
			throw new InvalidOptionException("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		} else if ( this.producerType.equals("kinesis") && this.kinesisStream == null) {
			throw new InvalidOptionException("please specify a stream name for kinesis", "kinesis_stream");
		} else if (this.producerType.equals("sqs") && this.sqsQueueUri == null) {
			throw new InvalidOptionException("please specify a queue uri for sqs", "sqs_queue_uri");
		}

		if ( !this.bootstrapperType.equals("async")
				&& !this.bootstrapperType.equals("sync")
				&& !this.bootstrapperType.equals("none") ) {
			throw new InvalidOptionException("please specify --bootstrapper=async|sync|none", "--bootstrapper");
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
				throw new InvalidOptionException("Please specify all of: replication_host, replication_user, replication_password", "--replication");
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
			throw new InvalidOptionException("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (outputConfig.includesGtidPosition && !gtidMode) {
			throw new InvalidOptionException("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (this.schemaMysql.host != null) {
			if (this.schemaMysql.user == null || this.schemaMysql.password == null) {
				throw new InvalidOptionException("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (this.replicationMysql.host == null) {
				throw new InvalidOptionException("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (this.schemaMysql.sslMode == null) {
			this.schemaMysql.sslMode = this.maxwellMysql.sslMode;
		}

		if ( this.filter == null ) {
			try {
				this.filter = new MaxwellFilter(
					includeDatabases,
					excludeDatabases,
					includeTables,
					excludeTables,
					blacklistDatabases,
					blacklistTables,
					includeColumnValues
				);
			} catch (MaxwellInvalidFilterException e) {
				throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
			}
		}

		if ( this.metricsDatadogType.contains("http") && StringUtils.isEmpty(this.metricsDatadogAPIKey) ) {
			throw new InvalidOptionException("please specify metrics_datadog_apikey when metrics_datadog_type = http");
		}

		if ( this.excludeColumns != null ) {
			for ( String s : this.excludeColumns.split(",") ) {
				try {
					outputConfig.excludeColumns.add(compileStringToPattern(s));
				} catch ( MaxwellInvalidFilterException e ) {
					throw new InvalidUsageException("invalid exclude_columns: '" + this.excludeColumns + "': " + e.getMessage());
				}
			}
		}

		if (outputConfig.encryptionEnabled() && outputConfig.secretKey == null)
			throw new InvalidUsageException("--secret_key required");

		if ( !maxwellMysql.sameServerAs(replicationMysql) && !this.bootstrapperType.equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.bootstrapperType = "none";
		}

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
			throw new InvalidOptionException("please specify --producer_partition_by=database|table|primary_key|column", "producer_partition_by");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionColumns) ) {
			throw new InvalidOptionException("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if ( this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionFallback) ) {
			throw new InvalidOptionException("please specify --producer_partition_by_fallback=[database, table, primary_key] when using producer_partition_by=column", "producer_partition_by_fallback");
		}

	}

	public Properties getKafkaProperties() {
		return this.kafkaProperties;
	}

	public static Pattern compileStringToPattern(String name) throws MaxwellInvalidFilterException {
		name = name.trim();
		if ( name.startsWith("/") ) {
			if ( !name.endsWith("/") ) {
				throw new MaxwellInvalidFilterException("Invalid regular expression: " + name);
			}
			return Pattern.compile(name.substring(1, name.length() - 1));
		} else {
			return Pattern.compile("^" + Pattern.quote(name) + "$");
		}
	}
}
