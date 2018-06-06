package com.zendesk.maxwell.core.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.replication.Position;
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

	private String bootstrapperType;

	private String producerPartitionKey;
	private String producerPartitionColumns;
	private String producerPartitionFallback;

	private Long producerAckTimeout;

	private final BaseMaxwellOutputConfig outputConfig;
	private String logLevel;

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

	private BaseMaxwellDiagnosticConfig diagnosticConfig;

	private String clientID;
	private Long replicaServerID;

	private Position initPosition;
	private boolean replayMode;
	private boolean masterRecovery;
	private boolean ignoreProducerError;


	public BaseMaxwellConfig() {
		this.customProducerProperties = new Properties();
		this.setReplayMode(false);
		this.setReplicationMysql(new BaseMaxwellMysqlConfig());
		this.setMaxwellMysql(new BaseMaxwellMysqlConfig());
		this.setSchemaMysql(new BaseMaxwellMysqlConfig());
		this.setMasterRecovery(false);
		this.setGtidMode(false);
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.outputConfig = new BaseMaxwellOutputConfig();
	}

	@Override
	public void validate() {
		validatePartitionBy();

		if ( !this.getBootstrapperType().equals("async")
				&& !this.getBootstrapperType().equals("sync")
				&& !this.getBootstrapperType().equals("none") ) {
			throw new InvalidOptionException("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if (this.getMaxwellMysql().getSslMode() == null) {
			this.getMaxwellMysql().setSslMode(SSLMode.DISABLED);
		}

		if ( this.getMaxwellMysql().getHost() == null ) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			this.getMaxwellMysql().setHost("localhost");
		}

		if ( this.getReplicationMysql().getHost() == null
				|| this.getReplicationMysql().getUser() == null ) {

			if (this.getReplicationMysql().getHost() != null
					|| this.getReplicationMysql().getUser() != null
					|| this.getReplicationMysql().getPassword() != null) {
				throw new InvalidOptionException("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			this.setReplicationMysql(new BaseMaxwellMysqlConfig(
					this.getMaxwellMysql().getHost(),
					this.getMaxwellMysql().getPort(),
				null,
					this.getMaxwellMysql().getUser(),
					this.getMaxwellMysql().getPassword(),
					this.getMaxwellMysql().getSslMode()
			));

			this.getReplicationMysql().setJdbcOptions(this.getMaxwellMysql().getJdbcOptions());
		}

		if (this.getReplicationMysql().getSslMode() == null) {
			this.getReplicationMysql().setSslMode(this.getMaxwellMysql().getSslMode());
		}

		if (getGtidMode() && isMasterRecovery()) {
			throw new InvalidOptionException("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (getOutputConfig().isIncludesGtidPosition() && !getGtidMode()) {
			throw new InvalidOptionException("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (this.getSchemaMysql().getHost() != null) {
			if (this.getSchemaMysql().getUser() == null || this.getSchemaMysql().getPassword() == null) {
				throw new InvalidOptionException("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (this.getReplicationMysql().getHost() == null) {
				throw new InvalidOptionException("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (this.getSchemaMysql().getSslMode() == null) {
			this.getSchemaMysql().setSslMode(this.getMaxwellMysql().getSslMode());
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
					getOutputConfig().getExcludeColumns().add(MaxwellConfig.compileStringToPattern(s));
				} catch ( MaxwellInvalidFilterException e ) {
					throw new InvalidUsageException("invalid exclude_columns: '" + this.getExcludeColumns() + "': " + e.getMessage());
				}
			}
		}

		if (getOutputConfig().isEncryptionEnabled() && getOutputConfig().getSecretKey() == null)
			throw new InvalidUsageException("--secret_key required");

		if ( !getMaxwellMysql().isSameServerAs(getReplicationMysql()) && !this.getBootstrapperType().equals("none") ) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			this.setBootstrapperType("none");
		}
	}

	private void validatePartitionBy() {
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

	@Override
	public BaseMaxwellMysqlConfig getReplicationMysql() {
		return replicationMysql;
	}

	public void setReplicationMysql(BaseMaxwellMysqlConfig replicationMysql) {
		this.replicationMysql = replicationMysql;
	}

	@Override
	public BaseMaxwellMysqlConfig getSchemaMysql() {
		return schemaMysql;
	}

	public void setSchemaMysql(BaseMaxwellMysqlConfig schemaMysql) {
		this.schemaMysql = schemaMysql;
	}

	@Override
	public BaseMaxwellMysqlConfig getMaxwellMysql() {
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
	public String getIncludeDatabases() {
		return includeDatabases;
	}

	public void setIncludeDatabases(String includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

	@Override
	public String getExcludeDatabases() {
		return excludeDatabases;
	}

	public void setExcludeDatabases(String excludeDatabases) {
		this.excludeDatabases = excludeDatabases;
	}

	@Override
	public String getIncludeTables() {
		return includeTables;
	}

	public void setIncludeTables(String includeTables) {
		this.includeTables = includeTables;
	}

	@Override
	public String getExcludeTables() {
		return excludeTables;
	}

	public void setExcludeTables(String excludeTables) {
		this.excludeTables = excludeTables;
	}

	@Override
	public String getExcludeColumns() {
		return excludeColumns;
	}

	public void setExcludeColumns(String excludeColumns) {
		this.excludeColumns = excludeColumns;
	}

	@Override
	public String getBlacklistDatabases() {
		return blacklistDatabases;
	}

	public void setBlacklistDatabases(String blacklistDatabases) {
		this.blacklistDatabases = blacklistDatabases;
	}

	@Override
	public String getBlacklistTables() {
		return blacklistTables;
	}

	public void setBlacklistTables(String blacklistTables) {
		this.blacklistTables = blacklistTables;
	}

	@Override
	public String getIncludeColumnValues() {
		return includeColumnValues;
	}

	public void setIncludeColumnValues(String includeColumnValues) {
		this.includeColumnValues = includeColumnValues;
	}

	@Override
	public ProducerFactory getProducerFactory() {
		return producerFactory;
	}

	public void setProducerFactory(ProducerFactory producerFactory) {
		this.producerFactory = producerFactory;
	}

	@Override
	public Properties getCustomProducerProperties() {
		return customProducerProperties;
	}

	@Override
	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}

	@Override
	public String getBootstrapperType() {
		return bootstrapperType;
	}

	public void setBootstrapperType(String bootstrapperType) {
		this.bootstrapperType = bootstrapperType;
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
	public BaseMaxwellOutputConfig getOutputConfig() {
		return outputConfig;
	}

	@Override
	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
	}

	@Override
	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	@Override
	public HealthCheckRegistry getHealthCheckRegistry() {
		return healthCheckRegistry;
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
	public BaseMaxwellDiagnosticConfig getDiagnosticConfig() {
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

	@Override
	public boolean isIgnoreProducerError() {
		return ignoreProducerError;
	}

	public void setIgnoreProducerError(boolean ignoreProducerError) {
		this.ignoreProducerError = ignoreProducerError;
	}

}
