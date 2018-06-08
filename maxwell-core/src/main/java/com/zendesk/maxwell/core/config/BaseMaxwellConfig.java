package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.*;
import com.zendesk.maxwell.api.producer.ProducerConfiguration;
import com.zendesk.maxwell.api.producer.PropertiesProducerConfiguration;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.producer.impl.stdout.StdoutProducerFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BaseMaxwellConfig implements MaxwellConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(BaseMaxwellConfig.class);

	private BaseMaxwellMysqlConfig replicationMysql;
	private BaseMaxwellMysqlConfig schemaMysql;
	private BaseMaxwellMysqlConfig maxwellMysql;
	private MaxwellFilter filter;
	private Boolean gtidMode;
	private String databaseName;

	private String producerFactory; // customProducerFactory has precedence over producerType
	private final Properties customProducerProperties;

	private String bootstrapperType;

	private String producerType;
	private ProducerConfiguration producerConfiguration;
	private boolean ignoreProducerError;

	private String producerPartitionKey;
	private String producerPartitionColumns;
	private String producerPartitionFallback;

	private Long producerAckTimeout;

	private MaxwellOutputConfig outputConfig;
	private String logLevel;

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

	private BaseMaxwellDiagnosticConfig diagnosticConfig;

	private String clientID;
	private Long replicaServerID;

	private Position initPosition;
	private boolean replayMode;
	private boolean masterRecovery;

	public BaseMaxwellConfig() {
		this.setReplicationMysql(new BaseMaxwellMysqlConfig());
		this.setMaxwellMysql(new BaseMaxwellMysqlConfig());
		this.setSchemaMysql(new BaseMaxwellMysqlConfig());
		this.setGtidMode(System.getenv(MaxwellConfig.GTID_MODE_ENV) != null);

		this.setDatabaseName(DEFAULT_DATABASE_NAME);
		this.setBootstrapperType(DEFAULT_BOOTSTRAPPER_TYPE);
		this.setClientID(DEFAULT_CLIENT_ID);

		this.setReplicaServerID(DEFAULT_REPLICA_SERVER_ID);
		this.setReplayMode(DEFAULT_REPLICATION_REPLAY_MODE);
		this.setMasterRecovery(DEFAULT_REPLICATION_MASTER_RECOVERY);

		this.setIgnoreProducerError(DEFAULT_PRODUCER_IGNORE_ERROR);
		this.setProducerAckTimeout(DEFAULT_PRODUCER_ACK_TIMEOUT);
		this.setProducerPartitionKey(DEFAULT_PRODUCER_PARTITION_KEY);
		this.setProducerType(DEFAULT_PRODUCER_TYPE);
		this.setProducerFactory(StdoutProducerFactory.class.getCanonicalName());
		this.setProducerConfiguration(new PropertiesProducerConfiguration());
		this.customProducerProperties = new Properties();

		this.setHttpPort(DEFAULT_HTTP_PORT);
		this.setHttpPathPrefix(DEFAULT_HTTP_PATH_PREFIX);

		this.setMetricsPrefix(DEFAULT_METRICS_PREFIX);
		this.setMetricsSlf4jInterval(DEFAULT_METRITCS_SLF4J_INTERVAL);
		this.setMetricsDatadogType(DEFAULT_METRICS_DATADOG_TYPE);
		this.setMetricsDatadogTags(DEFAULT_METRICS_DATADOG_TAGS);
		this.setMetricsDatadogAPIKey(DEFAULT_METRICS_DATADOG_APIKEY);
		this.setMetricsDatadogHost(DEFAULT_METRICS_DATADOG_HOST);
		this.setMetricsDatadogPort(DEFAULT_METRICS_DATADOG_PORT);
		this.setMetricsDatadogInterval(DEFAULT_METRICS_DATADOG_INTERVAL);
		this.setMetricsJvm(DEFAULT_METRCS_JVM);

		this.setMasterRecovery(false);
		this.setFilter(new BaseMaxwellFilter());
		this.setDiagnosticConfig(new BaseMaxwellDiagnosticConfig());
		this.setOutputConfig(new BaseMaxwellOutputConfig());
	}

	@Override
	public void validate() {
		validatePartitionBy();

		if (!bootstrapperType.equals("async") && !bootstrapperType.equals("sync") && !bootstrapperType.equals("none")) {
			throw new InvalidOptionException("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if (maxwellMysql.getSslMode() == null) {
			maxwellMysql.setSslMode(SSLMode.DISABLED);
		}

		if (maxwellMysql.getHost() == null) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			maxwellMysql.setHost("localhost");
		}

		if (replicationMysql.getHost() == null || replicationMysql.getUser() == null) {
			if (replicationMysql.getHost() != null || replicationMysql.getUser() != null || replicationMysql.getPassword() != null) {
				throw new InvalidOptionException("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			replicationMysql = new BaseMaxwellMysqlConfig(maxwellMysql.getHost(), maxwellMysql.getPort(), null, maxwellMysql.getUser(), maxwellMysql.getPassword(), maxwellMysql.getSslMode());
			replicationMysql.setJdbcOptions(maxwellMysql.getJdbcOptions());
		}

		if (replicationMysql.getSslMode() == null) {
			replicationMysql.setSslMode(maxwellMysql.getSslMode());
		}

		if (gtidMode && masterRecovery) {
			throw new InvalidOptionException("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (getOutputConfig().isIncludesGtidPosition() && !getGtidMode()) {
			throw new InvalidOptionException("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (schemaMysql.getHost() != null) {
			if (schemaMysql.getUser() == null || schemaMysql.getPassword() == null) {
				throw new InvalidOptionException("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (replicationMysql.getHost() == null) {
				throw new InvalidOptionException("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (schemaMysql.getSslMode() == null) {
			schemaMysql.setSslMode(maxwellMysql.getSslMode());
		}

		if (metricsDatadogType != null && metricsDatadogType.contains("http") && StringUtils.isEmpty(metricsDatadogAPIKey)) {
			throw new InvalidOptionException("please specify metrics_datadog_apikey when metrics_datadog_type = http");
		}

		if (getOutputConfig().isEncryptionEnabled() && getOutputConfig().getSecretKey() == null)
			throw new InvalidUsageException("--secret_key required");

		if (!maxwellMysql.isSameServerAs(replicationMysql) && !bootstrapperType.equals("none")) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			bootstrapperType = "none";
		}
	}

	private void validatePartitionBy() {
		String[] validPartitionBy = {"database", "table", "primary_key", "column"};
		if (this.getProducerPartitionKey() == null) {
			this.setProducerPartitionKey("database");
		} else if (!ArrayUtils.contains(validPartitionBy, this.getProducerPartitionKey())) {
			throw new InvalidOptionException("please specify --producer_partition_by=database|table|primary_key|column", "producer_partition_by");
		} else if (this.getProducerPartitionKey().equals("column") && StringUtils.isEmpty(this.getProducerPartitionColumns())) {
			throw new InvalidOptionException("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if (this.getProducerPartitionKey().equals("column") && StringUtils.isEmpty(this.getProducerPartitionFallback())) {
			throw new InvalidOptionException("please specify --producer_partition_by_fallback=[database, table, primary_key] when using producer_partition_by=column", "producer_partition_by_fallback");
		}

	}

	@Override
	public MaxwellMysqlConfig getReplicationMysql() {
		return replicationMysql;
	}

	public void setReplicationMysql(BaseMaxwellMysqlConfig replicationMysql) {
		this.replicationMysql = replicationMysql;
	}

	@Override
	public MaxwellMysqlConfig getSchemaMysql() {
		return schemaMysql;
	}

	public void setSchemaMysql(BaseMaxwellMysqlConfig schemaMysql) {
		this.schemaMysql = schemaMysql;
	}

	@Override
	public MaxwellMysqlConfig getMaxwellMysql() {
		return maxwellMysql;
	}

	public void setMaxwellMysql(BaseMaxwellMysqlConfig maxwellMysql) {
		this.maxwellMysql = maxwellMysql;
	}

	@Override
	public MaxwellFilter getFilter() {
		return filter;
	}

	public void setFilter(MaxwellFilter filter) {
		this.filter = filter;
	}

	@Override
	public Boolean getGtidMode() {
		return gtidMode;
	}

	public void setGtidMode(Boolean gtidMode) {
		this.gtidMode = gtidMode;
	}

	@Override
	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	@Override
	public String getBootstrapperType() {
		return bootstrapperType;
	}

	public void setBootstrapperType(String bootstrapperType) {
		this.bootstrapperType = bootstrapperType;
	}

	@Override
	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}

	@Override
	public ProducerConfiguration getProducerConfiguration() {
		return producerConfiguration;
	}

	public void setProducerConfiguration(ProducerConfiguration producerConfiguration) {
		this.producerConfiguration = producerConfiguration;
	}

	@Override
	public String getProducerFactory() {
		return producerFactory;
	}

	public void setProducerFactory(String producerFactory) {
		this.producerFactory = producerFactory;
	}

	@Override
	public Properties getCustomProducerProperties() {
		return customProducerProperties;
	}

	@Override
	public boolean isIgnoreProducerError() {
		return ignoreProducerError;
	}

	public void setIgnoreProducerError(boolean ignoreProducerError) {
		this.ignoreProducerError = ignoreProducerError;
	}

	@Override
	public String getProducerPartitionKey() {
		return producerPartitionKey;
	}

	@Override
	public void setProducerPartitionKey(String producerPartitionKey) {
		this.producerPartitionKey = producerPartitionKey;
	}

	@Override
	public String getProducerPartitionColumns() {
		return producerPartitionColumns;
	}

	@Override
	public void setProducerPartitionColumns(String producerPartitionColumns) {
		this.producerPartitionColumns = producerPartitionColumns;
	}

	@Override
	public String getProducerPartitionFallback() {
		return producerPartitionFallback;
	}

	@Override
	public void setProducerPartitionFallback(String producerPartitionFallback) {
		this.producerPartitionFallback = producerPartitionFallback;
	}

	@Override
	public Long getProducerAckTimeout() {
		return producerAckTimeout;
	}

	public void setProducerAckTimeout(Long producerAckTimeout) {
		this.producerAckTimeout = producerAckTimeout;
	}

	@Override
	public MaxwellOutputConfig getOutputConfig() {
		return outputConfig != null ? outputConfig : new BaseMaxwellOutputConfig();
	}

	public void setOutputConfig(MaxwellOutputConfig outputConfig) {
		this.outputConfig = outputConfig;
	}

	@Override
	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
	}

	@Override
	public int getHttpPort() {
		return httpPort;
	}

	public void setHttpPort(int httpPort) {
		this.httpPort = httpPort;
	}

	@Override
	public String getHttpBindAddress() {
		return httpBindAddress;
	}

	public void setHttpBindAddress(String httpBindAddress) {
		this.httpBindAddress = httpBindAddress;
	}

	@Override
	public String getHttpPathPrefix() {
		return httpPathPrefix;
	}

	public void setHttpPathPrefix(String httpPathPrefix) {
		this.httpPathPrefix = httpPathPrefix;
	}

	@Override
	public String getMetricsPrefix() {
		return metricsPrefix;
	}

	public void setMetricsPrefix(String metricsPrefix) {
		this.metricsPrefix = metricsPrefix;
	}

	@Override
	public String getMetricsReportingType() {
		return metricsReportingType;
	}

	public void setMetricsReportingType(String metricsReportingType) {
		this.metricsReportingType = metricsReportingType;
	}

	@Override
	public Long getMetricsSlf4jInterval() {
		return metricsSlf4jInterval;
	}

	public void setMetricsSlf4jInterval(Long metricsSlf4jInterval) {
		this.metricsSlf4jInterval = metricsSlf4jInterval;
	}

	@Override
	public String getMetricsDatadogType() {
		return metricsDatadogType;
	}

	public void setMetricsDatadogType(String metricsDatadogType) {
		this.metricsDatadogType = metricsDatadogType;
	}

	@Override
	public String getMetricsDatadogTags() {
		return metricsDatadogTags;
	}

	public void setMetricsDatadogTags(String metricsDatadogTags) {
		this.metricsDatadogTags = metricsDatadogTags;
	}

	@Override
	public String getMetricsDatadogAPIKey() {
		return metricsDatadogAPIKey;
	}

	public void setMetricsDatadogAPIKey(String metricsDatadogAPIKey) {
		this.metricsDatadogAPIKey = metricsDatadogAPIKey;
	}

	@Override
	public String getMetricsDatadogHost() {
		return metricsDatadogHost;
	}

	public void setMetricsDatadogHost(String metricsDatadogHost) {
		this.metricsDatadogHost = metricsDatadogHost;
	}

	@Override
	public int getMetricsDatadogPort() {
		return metricsDatadogPort;
	}

	public void setMetricsDatadogPort(int metricsDatadogPort) {
		this.metricsDatadogPort = metricsDatadogPort;
	}

	@Override
	public Long getMetricsDatadogInterval() {
		return metricsDatadogInterval;
	}

	public void setMetricsDatadogInterval(Long metricsDatadogInterval) {
		this.metricsDatadogInterval = metricsDatadogInterval;
	}

	@Override
	public boolean isMetricsJvm() {
		return metricsJvm;
	}

	public void setMetricsJvm(boolean metricsJvm) {
		this.metricsJvm = metricsJvm;
	}

	@Override
	public MaxwellDiagnosticConfig getDiagnosticConfig() {
		return diagnosticConfig;
	}

	public void setDiagnosticConfig(BaseMaxwellDiagnosticConfig diagnosticConfig) {
		this.diagnosticConfig = diagnosticConfig;
	}

	@Override
	public String getClientID() {
		return clientID;
	}

	public void setClientID(String clientID) {
		this.clientID = clientID;
	}

	@Override
	public Long getReplicaServerID() {
		return replicaServerID;
	}

	public void setReplicaServerID(Long replicaServerID) {
		this.replicaServerID = replicaServerID;
	}

	@Override
	public Position getInitPosition() {
		return initPosition;
	}

	public void setInitPosition(Position initPosition) {
		this.initPosition = initPosition;
	}

	@Override
	public boolean isReplayMode() {
		return replayMode;
	}

	public void setReplayMode(boolean replayMode) {
		this.replayMode = replayMode;
	}

	@Override
	public boolean isMasterRecovery() {
		return masterRecovery;
	}

	public void setMasterRecovery(boolean masterRecovery) {
		this.masterRecovery = masterRecovery;
	}

}
