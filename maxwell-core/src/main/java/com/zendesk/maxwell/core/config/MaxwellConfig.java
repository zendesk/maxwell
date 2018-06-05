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

	private String bootstrapperType;

	private String producerPartitionKey;
	private String producerPartitionColumns;
	private String producerPartitionFallback;

	private Long producerAckTimeout;

	private final MaxwellOutputConfig outputConfig;
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

	private MaxwellDiagnosticContext.Config diagnosticConfig;

	private String clientID;
	private Long replicaServerID;

	private Position initPosition;
	private boolean replayMode;
	private boolean masterRecovery;
	private boolean ignoreProducerError;


	public MaxwellConfig() {
		this.customProducerProperties = new Properties();
		this.setReplayMode(false);
		this.setReplicationMysql(new MaxwellMysqlConfig());
		this.setMaxwellMysql(new MaxwellMysqlConfig());
		this.setSchemaMysql(new MaxwellMysqlConfig());
		this.setMasterRecovery(false);
		this.setGtidMode(false);
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();
		this.outputConfig = new MaxwellOutputConfig();
	}

	public void validate() {
		validatePartitionBy();

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

	public String getBootstrapperType() {
		return bootstrapperType;
	}

	public void setBootstrapperType(String bootstrapperType) {
		this.bootstrapperType = bootstrapperType;
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

	public Long getProducerAckTimeout() {
		return producerAckTimeout;
	}

	public void setProducerAckTimeout(Long producerAckTimeout) {
		this.producerAckTimeout = producerAckTimeout;
	}

	public MaxwellOutputConfig getOutputConfig() {
		return outputConfig;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
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

}
