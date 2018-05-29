package com.zendesk.maxwell.core.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticContext;
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

	public static final String GTID_MODE_ENV = "GTID_MODE";

	private MaxwellMysqlConfig replicationMysql;
	private MaxwellMysqlConfig schemaMysql;

	private MaxwellMysqlConfig maxwellMysql;
	private MaxwellFilter filter;
	private Boolean gtidMode;

	private String databaseName;

	private String includeDatabases;
	private String excludeDatabases;
	private String includeTables;
	private String excludeTables;
	private String excludeColumns;
	private String blacklistDatabases;
	private String blacklistTables;
	private String includeColumnValues;

	private ProducerFactory producerFactory; // producerFactory has precedence over producerType
	private final Properties customProducerProperties;
	private String producerType;

	private final Properties kafkaProperties;
	private String kafkaTopic;
	private String ddlKafkaTopic;
	private String kafkaKeyFormat;
	private String kafkaPartitionHash;
	private String kafkaPartitionKey;
	private String kafkaPartitionColumns;
	private String kafkaPartitionFallback;
	private String bootstrapperType;
	private int bufferedProducerSize;

	private String producerPartitionKey;
	private String producerPartitionColumns;
	private String producerPartitionFallback;

	private String kinesisStream;
	private boolean kinesisMd5Keys;

	private String sqsQueueUri;

	private String pubsubProjectId;
	private String pubsubTopic;
	private String ddlPubsubTopic;

	private Long producerAckTimeout;

	private String outputFile;
	private final MaxwellOutputConfig outputConfig;
	private String log_level;

	private final MetricRegistry metricRegistry;
	private final HealthCheckRegistry healthCheckRegistry;

	private int httpPort;
	private String httpBindAddress;
	private String httpPathPrefix;
	private String metricsPrefix;
	private String metricsReportingType;
	private Long metricsSlf4jInterval;
	private String metricsDatadogType;
	private String metricsDatadogTags;
	private String metricsDatadogAPIKey;
	private String metricsDatadogHost;
	private int metricsDatadogPort;
	private Long metricsDatadogInterval;
	private boolean metricsJvm;

	private MaxwellDiagnosticContext.Config diagnosticConfig;

	private String clientID;
	private Long replicaServerID;

	private Position initPosition;
	private boolean replayMode;
	private boolean masterRecovery;
	private boolean ignoreProducerError;

	private String rabbitmqUser;
	private String rabbitmqPass;
	private String rabbitmqHost;
	private int rabbitmqPort;
	private String rabbitmqVirtualHost;
	private String rabbitmqExchange;
	private String rabbitmqExchangeType;
	private boolean rabbitMqExchangeDurable;
	private boolean rabbitMqExchangeAutoDelete;
	private String rabbitmqRoutingKeyTemplate;
	private boolean rabbitmqMessagePersistent;
	private boolean rabbitmqDeclareExchange;

	private String redisHost;
	private int redisPort;
	private String redisAuth;
	private int redisDatabase;
	private String redisPubChannel;
	private String redisListKey;
	private String redisType;

	public MaxwellConfig() {
		this.customProducerProperties = new Properties();
		this.kafkaProperties = new Properties();
		this.setReplayMode(false);
		this.setReplicationMysql(new MaxwellMysqlConfig());
		this.setMaxwellMysql(new MaxwellMysqlConfig());
		this.setSchemaMysql(new MaxwellMysqlConfig());
		this.setMasterRecovery(false);
		this.setGtidMode(false);
		this.setBufferedProducerSize(200);
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.outputConfig = new MaxwellOutputConfig();
	}

	public void validate() {
		validatePartitionBy();

		if ( this.getProducerType().equals("kafka") ) {
			if ( !this.getKafkaProperties().containsKey("bootstrap.servers") ) {
				throw new InvalidOptionException("You must specify kafka.bootstrap.servers for the kafka producer!", "kafka");
			}

			if ( this.getKafkaPartitionHash() == null ) {
				this.setKafkaPartitionHash("default");
			} else if ( !this.getKafkaPartitionHash().equals("default")
					&& !this.getKafkaPartitionHash().equals("murmur3") ) {
				throw new InvalidOptionException("please specify --kafka_partition_hash=default|murmur3", "kafka_partition_hash");
			}

			if ( !this.getKafkaKeyFormat().equals("hash") && !this.getKafkaKeyFormat().equals("array") )
				throw new InvalidOptionException("invalid kafka_key_format: " + this.getKafkaKeyFormat(), "kafka_key_format");

		} else if ( this.getProducerType().equals("file")
				&& this.getOutputFile() == null) {
			throw new InvalidOptionException("please specify --output_file=FILE to use the file producer", "--producer", "--output_file");
		} else if ( this.getProducerType().equals("kinesis") && this.getKinesisStream() == null) {
			throw new InvalidOptionException("please specify a stream name for kinesis", "kinesis_stream");
		} else if (this.getProducerType().equals("sqs") && this.getSqsQueueUri() == null) {
			throw new InvalidOptionException("please specify a queue uri for sqs", "sqs_queue_uri");
		}

		if ( !this.getBootstrapperType().equals("async")
				&& !this.getBootstrapperType().equals("sync")
				&& !this.getBootstrapperType().equals("none") ) {
			throw new InvalidOptionException("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if (this.getMaxwellMysql().sslMode == null) {
			this.getMaxwellMysql().sslMode = SSLMode.DISABLED;
		}

		if ( this.getMaxwellMysql().host == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.getMaxwellMysql().host = "localhost";
		}

		if ( this.getReplicationMysql().host == null
				|| this.getReplicationMysql().user == null ) {

			if (this.getReplicationMysql().host != null
					|| this.getReplicationMysql().user != null
					|| this.getReplicationMysql().password != null) {
				throw new InvalidOptionException("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			this.setReplicationMysql(new MaxwellMysqlConfig(
				this.getMaxwellMysql().host,
				this.getMaxwellMysql().port,
				null,
				this.getMaxwellMysql().user,
				this.getMaxwellMysql().password,
				this.getMaxwellMysql().sslMode
			));

			this.getReplicationMysql().jdbcOptions = this.getMaxwellMysql().jdbcOptions;
		}

		if (this.getReplicationMysql().sslMode == null) {
			this.getReplicationMysql().sslMode = this.getMaxwellMysql().sslMode;
		}

		if (getGtidMode() && isMasterRecovery()) {
			throw new InvalidOptionException("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (getOutputConfig().includesGtidPosition && !getGtidMode()) {
			throw new InvalidOptionException("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (this.getSchemaMysql().host != null) {
			if (this.getSchemaMysql().user == null || this.getSchemaMysql().password == null) {
				throw new InvalidOptionException("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (this.getReplicationMysql().host == null) {
				throw new InvalidOptionException("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (this.getSchemaMysql().sslMode == null) {
			this.getSchemaMysql().sslMode = this.getMaxwellMysql().sslMode;
		}

		if ( this.getFilter() == null ) {
			try {
				this.setFilter(new MaxwellFilter(
						getIncludeDatabases(),
						getExcludeDatabases(),
						getIncludeTables(),
						getExcludeTables(),
						getBlacklistDatabases(),
						getBlacklistTables(),
						getIncludeColumnValues()
				));
			} catch (MaxwellInvalidFilterException e) {
				throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
			}
		}

		if ( this.getMetricsDatadogType().contains("http") && StringUtils.isEmpty(this.getMetricsDatadogAPIKey()) ) {
			throw new InvalidOptionException("please specify metrics_datadog_apikey when metrics_datadog_type = http");
		}

		if ( this.getExcludeColumns() != null ) {
			for ( String s : this.getExcludeColumns().split(",") ) {
				try {
					getOutputConfig().excludeColumns.add(compileStringToPattern(s));
				} catch ( MaxwellInvalidFilterException e ) {
					throw new InvalidUsageException("invalid exclude_columns: '" + this.getExcludeColumns() + "': " + e.getMessage());
				}
			}
		}

		if (getOutputConfig().encryptionEnabled() && getOutputConfig().secretKey == null)
			throw new InvalidUsageException("--secret_key required");

		if ( !getMaxwellMysql().sameServerAs(getReplicationMysql()) && !this.getBootstrapperType().equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.setBootstrapperType("none");
		}

	}

	private void validatePartitionBy() {
		if ( this.getProducerPartitionKey() == null && this.getKafkaPartitionKey() != null ) {
			LOGGER.warn("kafka_partition_by is deprecated, please use producer_partition_by");
			this.setProducerPartitionKey(this.getKafkaPartitionKey());
		}

		if ( this.getProducerPartitionColumns() == null && this.getKafkaPartitionColumns() != null) {
			LOGGER.warn("kafka_partition_columns is deprecated, please use producer_partition_columns");
			this.setProducerPartitionColumns(this.getKafkaPartitionColumns());
		}

		if ( this.getProducerPartitionFallback() == null && this.getKafkaPartitionFallback() != null ) {
			LOGGER.warn("kafka_partition_by_fallback is deprecated, please use producer_partition_by_fallback");
			this.setProducerPartitionFallback(this.getKafkaPartitionFallback());
		}

		String[] validPartitionBy = {"database", "table", "primary_key", "column"};
		if ( this.getProducerPartitionKey() == null ) {
			this.setProducerPartitionKey("database");
		} else if ( !ArrayUtils.contains(validPartitionBy, this.getProducerPartitionKey()) ) {
			throw new InvalidOptionException("please specify --producer_partition_by=database|table|primary_key|column", "producer_partition_by");
		} else if ( this.getProducerPartitionKey().equals("column") && StringUtils.isEmpty(this.getProducerPartitionColumns()) ) {
			throw new InvalidOptionException("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if ( this.getProducerPartitionKey().equals("column") && StringUtils.isEmpty(this.getProducerPartitionFallback()) ) {
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

	public MaxwellMysqlConfig getReplicationMysql() {
		return replicationMysql;
	}

	public void setReplicationMysql(MaxwellMysqlConfig replicationMysql) {
		this.replicationMysql = replicationMysql;
	}

	public MaxwellMysqlConfig getSchemaMysql() {
		return schemaMysql;
	}

	public void setSchemaMysql(MaxwellMysqlConfig schemaMysql) {
		this.schemaMysql = schemaMysql;
	}

	public MaxwellMysqlConfig getMaxwellMysql() {
		return maxwellMysql;
	}

	public void setMaxwellMysql(MaxwellMysqlConfig maxwellMysql) {
		this.maxwellMysql = maxwellMysql;
	}

	public MaxwellFilter getFilter() {
		return filter;
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}

	public Boolean getGtidMode() {
		return gtidMode;
	}

	public void setGtidMode(Boolean gtidMode) {
		this.gtidMode = gtidMode;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getIncludeDatabases() {
		return includeDatabases;
	}

	public void setIncludeDatabases(String includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

	public String getExcludeDatabases() {
		return excludeDatabases;
	}

	public void setExcludeDatabases(String excludeDatabases) {
		this.excludeDatabases = excludeDatabases;
	}

	public String getIncludeTables() {
		return includeTables;
	}

	public void setIncludeTables(String includeTables) {
		this.includeTables = includeTables;
	}

	public String getExcludeTables() {
		return excludeTables;
	}

	public void setExcludeTables(String excludeTables) {
		this.excludeTables = excludeTables;
	}

	public String getExcludeColumns() {
		return excludeColumns;
	}

	public void setExcludeColumns(String excludeColumns) {
		this.excludeColumns = excludeColumns;
	}

	public String getBlacklistDatabases() {
		return blacklistDatabases;
	}

	public void setBlacklistDatabases(String blacklistDatabases) {
		this.blacklistDatabases = blacklistDatabases;
	}

	public String getBlacklistTables() {
		return blacklistTables;
	}

	public void setBlacklistTables(String blacklistTables) {
		this.blacklistTables = blacklistTables;
	}

	public String getIncludeColumnValues() {
		return includeColumnValues;
	}

	public void setIncludeColumnValues(String includeColumnValues) {
		this.includeColumnValues = includeColumnValues;
	}

	public ProducerFactory getProducerFactory() {
		return producerFactory;
	}

	public void setProducerFactory(ProducerFactory producerFactory) {
		this.producerFactory = producerFactory;
	}

	public Properties getCustomProducerProperties() {
		return customProducerProperties;
	}

	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public String getDdlKafkaTopic() {
		return ddlKafkaTopic;
	}

	public void setDdlKafkaTopic(String ddlKafkaTopic) {
		this.ddlKafkaTopic = ddlKafkaTopic;
	}

	public String getKafkaKeyFormat() {
		return kafkaKeyFormat;
	}

	public void setKafkaKeyFormat(String kafkaKeyFormat) {
		this.kafkaKeyFormat = kafkaKeyFormat;
	}

	public String getKafkaPartitionHash() {
		return kafkaPartitionHash;
	}

	public void setKafkaPartitionHash(String kafkaPartitionHash) {
		this.kafkaPartitionHash = kafkaPartitionHash;
	}

	public String getKafkaPartitionKey() {
		return kafkaPartitionKey;
	}

	public void setKafkaPartitionKey(String kafkaPartitionKey) {
		this.kafkaPartitionKey = kafkaPartitionKey;
	}

	public String getKafkaPartitionColumns() {
		return kafkaPartitionColumns;
	}

	public void setKafkaPartitionColumns(String kafkaPartitionColumns) {
		this.kafkaPartitionColumns = kafkaPartitionColumns;
	}

	public String getKafkaPartitionFallback() {
		return kafkaPartitionFallback;
	}

	public void setKafkaPartitionFallback(String kafkaPartitionFallback) {
		this.kafkaPartitionFallback = kafkaPartitionFallback;
	}

	public String getBootstrapperType() {
		return bootstrapperType;
	}

	public void setBootstrapperType(String bootstrapperType) {
		this.bootstrapperType = bootstrapperType;
	}

	public int getBufferedProducerSize() {
		return bufferedProducerSize;
	}

	public void setBufferedProducerSize(int bufferedProducerSize) {
		this.bufferedProducerSize = bufferedProducerSize;
	}

	public String getProducerPartitionKey() {
		return producerPartitionKey;
	}

	public void setProducerPartitionKey(String producerPartitionKey) {
		this.producerPartitionKey = producerPartitionKey;
	}

	public String getProducerPartitionColumns() {
		return producerPartitionColumns;
	}

	public void setProducerPartitionColumns(String producerPartitionColumns) {
		this.producerPartitionColumns = producerPartitionColumns;
	}

	public String getProducerPartitionFallback() {
		return producerPartitionFallback;
	}

	public void setProducerPartitionFallback(String producerPartitionFallback) {
		this.producerPartitionFallback = producerPartitionFallback;
	}

	public String getKinesisStream() {
		return kinesisStream;
	}

	public void setKinesisStream(String kinesisStream) {
		this.kinesisStream = kinesisStream;
	}

	public boolean isKinesisMd5Keys() {
		return kinesisMd5Keys;
	}

	public void setKinesisMd5Keys(boolean kinesisMd5Keys) {
		this.kinesisMd5Keys = kinesisMd5Keys;
	}

	public String getSqsQueueUri() {
		return sqsQueueUri;
	}

	public void setSqsQueueUri(String sqsQueueUri) {
		this.sqsQueueUri = sqsQueueUri;
	}

	public String getPubsubProjectId() {
		return pubsubProjectId;
	}

	public void setPubsubProjectId(String pubsubProjectId) {
		this.pubsubProjectId = pubsubProjectId;
	}

	public String getPubsubTopic() {
		return pubsubTopic;
	}

	public void setPubsubTopic(String pubsubTopic) {
		this.pubsubTopic = pubsubTopic;
	}

	public String getDdlPubsubTopic() {
		return ddlPubsubTopic;
	}

	public void setDdlPubsubTopic(String ddlPubsubTopic) {
		this.ddlPubsubTopic = ddlPubsubTopic;
	}

	public Long getProducerAckTimeout() {
		return producerAckTimeout;
	}

	public void setProducerAckTimeout(Long producerAckTimeout) {
		this.producerAckTimeout = producerAckTimeout;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public MaxwellOutputConfig getOutputConfig() {
		return outputConfig;
	}

	public String getLog_level() {
		return log_level;
	}

	public void setLog_level(String log_level) {
		this.log_level = log_level;
	}

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public HealthCheckRegistry getHealthCheckRegistry() {
		return healthCheckRegistry;
	}

	public int getHttpPort() {
		return httpPort;
	}

	public void setHttpPort(int httpPort) {
		this.httpPort = httpPort;
	}

	public String getHttpBindAddress() {
		return httpBindAddress;
	}

	public void setHttpBindAddress(String httpBindAddress) {
		this.httpBindAddress = httpBindAddress;
	}

	public String getHttpPathPrefix() {
		return httpPathPrefix;
	}

	public void setHttpPathPrefix(String httpPathPrefix) {
		this.httpPathPrefix = httpPathPrefix;
	}

	public String getMetricsPrefix() {
		return metricsPrefix;
	}

	public void setMetricsPrefix(String metricsPrefix) {
		this.metricsPrefix = metricsPrefix;
	}

	public String getMetricsReportingType() {
		return metricsReportingType;
	}

	public void setMetricsReportingType(String metricsReportingType) {
		this.metricsReportingType = metricsReportingType;
	}

	public Long getMetricsSlf4jInterval() {
		return metricsSlf4jInterval;
	}

	public void setMetricsSlf4jInterval(Long metricsSlf4jInterval) {
		this.metricsSlf4jInterval = metricsSlf4jInterval;
	}

	public String getMetricsDatadogType() {
		return metricsDatadogType;
	}

	public void setMetricsDatadogType(String metricsDatadogType) {
		this.metricsDatadogType = metricsDatadogType;
	}

	public String getMetricsDatadogTags() {
		return metricsDatadogTags;
	}

	public void setMetricsDatadogTags(String metricsDatadogTags) {
		this.metricsDatadogTags = metricsDatadogTags;
	}

	public String getMetricsDatadogAPIKey() {
		return metricsDatadogAPIKey;
	}

	public void setMetricsDatadogAPIKey(String metricsDatadogAPIKey) {
		this.metricsDatadogAPIKey = metricsDatadogAPIKey;
	}

	public String getMetricsDatadogHost() {
		return metricsDatadogHost;
	}

	public void setMetricsDatadogHost(String metricsDatadogHost) {
		this.metricsDatadogHost = metricsDatadogHost;
	}

	public int getMetricsDatadogPort() {
		return metricsDatadogPort;
	}

	public void setMetricsDatadogPort(int metricsDatadogPort) {
		this.metricsDatadogPort = metricsDatadogPort;
	}

	public Long getMetricsDatadogInterval() {
		return metricsDatadogInterval;
	}

	public void setMetricsDatadogInterval(Long metricsDatadogInterval) {
		this.metricsDatadogInterval = metricsDatadogInterval;
	}

	public boolean isMetricsJvm() {
		return metricsJvm;
	}

	public void setMetricsJvm(boolean metricsJvm) {
		this.metricsJvm = metricsJvm;
	}

	public MaxwellDiagnosticContext.Config getDiagnosticConfig() {
		return diagnosticConfig;
	}

	public void setDiagnosticConfig(MaxwellDiagnosticContext.Config diagnosticConfig) {
		this.diagnosticConfig = diagnosticConfig;
	}

	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	public Long getReplicaServerID() {
		return replicaServerID;
	}

	public void setReplicaServerID(Long replicaServerID) {
		this.replicaServerID = replicaServerID;
	}

	public Position getInitPosition() {
		return initPosition;
	}

	public void setInitPosition(Position initPosition) {
		this.initPosition = initPosition;
	}

	public boolean isReplayMode() {
		return replayMode;
	}

	public void setReplayMode(boolean replayMode) {
		this.replayMode = replayMode;
	}

	public boolean isMasterRecovery() {
		return masterRecovery;
	}

	public void setMasterRecovery(boolean masterRecovery) {
		this.masterRecovery = masterRecovery;
	}

	public boolean isIgnoreProducerError() {
		return ignoreProducerError;
	}

	public void setIgnoreProducerError(boolean ignoreProducerError) {
		this.ignoreProducerError = ignoreProducerError;
	}

	public String getRabbitmqUser() {
		return rabbitmqUser;
	}

	public void setRabbitmqUser(String rabbitmqUser) {
		this.rabbitmqUser = rabbitmqUser;
	}

	public String getRabbitmqPass() {
		return rabbitmqPass;
	}

	public void setRabbitmqPass(String rabbitmqPass) {
		this.rabbitmqPass = rabbitmqPass;
	}

	public String getRabbitmqHost() {
		return rabbitmqHost;
	}

	public void setRabbitmqHost(String rabbitmqHost) {
		this.rabbitmqHost = rabbitmqHost;
	}

	public int getRabbitmqPort() {
		return rabbitmqPort;
	}

	public void setRabbitmqPort(int rabbitmqPort) {
		this.rabbitmqPort = rabbitmqPort;
	}

	public String getRabbitmqVirtualHost() {
		return rabbitmqVirtualHost;
	}

	public void setRabbitmqVirtualHost(String rabbitmqVirtualHost) {
		this.rabbitmqVirtualHost = rabbitmqVirtualHost;
	}

	public String getRabbitmqExchange() {
		return rabbitmqExchange;
	}

	public void setRabbitmqExchange(String rabbitmqExchange) {
		this.rabbitmqExchange = rabbitmqExchange;
	}

	public String getRabbitmqExchangeType() {
		return rabbitmqExchangeType;
	}

	public void setRabbitmqExchangeType(String rabbitmqExchangeType) {
		this.rabbitmqExchangeType = rabbitmqExchangeType;
	}

	public boolean isRabbitMqExchangeDurable() {
		return rabbitMqExchangeDurable;
	}

	public void setRabbitMqExchangeDurable(boolean rabbitMqExchangeDurable) {
		this.rabbitMqExchangeDurable = rabbitMqExchangeDurable;
	}

	public boolean isRabbitMqExchangeAutoDelete() {
		return rabbitMqExchangeAutoDelete;
	}

	public void setRabbitMqExchangeAutoDelete(boolean rabbitMqExchangeAutoDelete) {
		this.rabbitMqExchangeAutoDelete = rabbitMqExchangeAutoDelete;
	}

	public String getRabbitmqRoutingKeyTemplate() {
		return rabbitmqRoutingKeyTemplate;
	}

	public void setRabbitmqRoutingKeyTemplate(String rabbitmqRoutingKeyTemplate) {
		this.rabbitmqRoutingKeyTemplate = rabbitmqRoutingKeyTemplate;
	}

	public boolean isRabbitmqMessagePersistent() {
		return rabbitmqMessagePersistent;
	}

	public void setRabbitmqMessagePersistent(boolean rabbitmqMessagePersistent) {
		this.rabbitmqMessagePersistent = rabbitmqMessagePersistent;
	}

	public boolean isRabbitmqDeclareExchange() {
		return rabbitmqDeclareExchange;
	}

	public void setRabbitmqDeclareExchange(boolean rabbitmqDeclareExchange) {
		this.rabbitmqDeclareExchange = rabbitmqDeclareExchange;
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public String getRedisAuth() {
		return redisAuth;
	}

	public void setRedisAuth(String redisAuth) {
		this.redisAuth = redisAuth;
	}

	public int getRedisDatabase() {
		return redisDatabase;
	}

	public void setRedisDatabase(int redisDatabase) {
		this.redisDatabase = redisDatabase;
	}

	public String getRedisPubChannel() {
		return redisPubChannel;
	}

	public void setRedisPubChannel(String redisPubChannel) {
		this.redisPubChannel = redisPubChannel;
	}

	public String getRedisListKey() {
		return redisListKey;
	}

	public void setRedisListKey(String redisListKey) {
		this.redisListKey = redisListKey;
	}

	public String getRedisType() {
		return redisType;
	}

	public void setRedisType(String redisType) {
		this.redisType = redisType;
	}
}
