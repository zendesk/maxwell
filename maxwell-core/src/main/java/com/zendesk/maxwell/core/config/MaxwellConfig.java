package com.zendesk.maxwell.core.config;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.zendesk.maxwell.api.config.*;
import com.zendesk.maxwell.core.replication.Position;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MaxwellConfig {
	public final static String DEFAULT_DATABASE_NAME = "maxwell";
	public final static String DEFAULT_BOOTSTRAPPER_TYPE = "async";
	public final static String DEFAULT_CLIENT_ID = "maxwell";

	public final static long DEFAULT_REPLICA_SERVER_ID = 6379L;
	public final static boolean DEFAULT_REPLICATION_REPLAY_MODE = false;
	public final static boolean DEFAULT_REPLICATION_MASTER_RECOVERY = false;

	public final static boolean DEFAULT_PRODUCER_IGNORE_ERROR = true;
	public final static long DEFAULT_PRODUCER_ACK_TIMEOUT = 0L;
	public final static String DEFAULT_PRODUCER_PARTITION_KEY = "database";
	public final static String DEFAULT_PRODUCER_TYPE = "stdout";

	public final static String DEFAULT_METRICS_PREFIX = "MaxwellMetrics";
	public final static boolean DEFAULT_METRCS_JVM = false;

	public final static String GTID_MODE_ENV = "GTID_MODE";


	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfig.class);

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
	private boolean ignoreProducerError;

	private String producerPartitionKey;
	private String producerPartitionColumns;
	private String producerPartitionFallback;

	private Long producerAckTimeout;

	private MaxwellOutputConfig outputConfig;
	private String logLevel;

	private String metricsPrefix;
	private String metricsReportingType;
	private boolean metricsJvm;

	private String clientID;
	private Long replicaServerID;

	private Position initPosition;
	private boolean replayMode;
	private boolean masterRecovery;

	public MaxwellConfig() {
		this.setReplicationMysql(new BaseMaxwellMysqlConfig());
		this.setMaxwellMysql(new BaseMaxwellMysqlConfig());
		this.setSchemaMysql(new BaseMaxwellMysqlConfig());
		this.setGtidMode(System.getenv(GTID_MODE_ENV) != null);

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
		this.customProducerProperties = new Properties();

		this.setMetricsPrefix(DEFAULT_METRICS_PREFIX);
		this.setMetricsJvm(DEFAULT_METRCS_JVM);

		this.setMasterRecovery(false);
		this.setFilter(new BaseMaxwellFilter());
		this.setOutputConfig(new BaseMaxwellOutputConfig());
	}

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

	public MaxwellMysqlConfig getReplicationMysql() {
		return replicationMysql;
	}

	public void setReplicationMysql(BaseMaxwellMysqlConfig replicationMysql) {
		this.replicationMysql = replicationMysql;
	}

	public MaxwellMysqlConfig getSchemaMysql() {
		return schemaMysql;
	}

	public void setSchemaMysql(BaseMaxwellMysqlConfig schemaMysql) {
		this.schemaMysql = schemaMysql;
	}

	public MaxwellMysqlConfig getMaxwellMysql() {
		return maxwellMysql;
	}

	public void setMaxwellMysql(BaseMaxwellMysqlConfig maxwellMysql) {
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

	public String getBootstrapperType() {
		return bootstrapperType;
	}

	public void setBootstrapperType(String bootstrapperType) {
		this.bootstrapperType = bootstrapperType;
	}

	public String getProducerType() {
		return producerType;
	}

	public void setProducerType(String producerType) {
		this.producerType = producerType;
	}

	public String getProducerFactory() {
		return producerFactory;
	}

	public void setProducerFactory(String producerFactory) {
		this.producerFactory = producerFactory;
	}

	public Properties getCustomProducerProperties() {
		return customProducerProperties;
	}

	public boolean isIgnoreProducerError() {
		return ignoreProducerError;
	}

	public void setIgnoreProducerError(boolean ignoreProducerError) {
		this.ignoreProducerError = ignoreProducerError;
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
		return outputConfig != null ? outputConfig : new BaseMaxwellOutputConfig();
	}

	public void setOutputConfig(MaxwellOutputConfig outputConfig) {
		this.outputConfig = outputConfig;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
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

	public boolean isMetricsJvm() {
		return metricsJvm;
	}

	public void setMetricsJvm(boolean metricsJvm) {
		this.metricsJvm = metricsJvm;
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

}
