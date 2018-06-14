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

	public MaxwellMysqlConfig replicationMysql;
	public MaxwellMysqlConfig schemaMysql;
	public MaxwellMysqlConfig maxwellMysql;
	public MaxwellFilter filter;
	public Boolean gtidMode;
	public String databaseName;

	public String producerFactory; // customProducerFactory has precedence over producerType
	public final Properties customProducerProperties;

	public String bootstrapperType;

	public String producerType;
	public boolean ignoreProducerError;

	public String producerPartitionKey;
	public String producerPartitionColumns;
	public String producerPartitionFallback;

	public Long producerAckTimeout;

	public MaxwellOutputConfig outputConfig;
	public String logLevel;

	public String metricsPrefix;
	public String metricsReportingType;
	public boolean metricsJvmEnabled;

	public String clientID;
	public Long replicaServerID;

	public Position initPosition;
	public boolean replayModeEnabled;
	public boolean masterRecoveryEnabled;

	public MaxwellConfig() {
		this.replicationMysql = new MaxwellMysqlConfig();
		this.maxwellMysql = new MaxwellMysqlConfig();
		this.schemaMysql = new MaxwellMysqlConfig();
		this.gtidMode = System.getenv(GTID_MODE_ENV) != null;

		this.databaseName = DEFAULT_DATABASE_NAME;
		this.bootstrapperType = DEFAULT_BOOTSTRAPPER_TYPE;
		this.clientID = DEFAULT_CLIENT_ID;

		this.replicaServerID = DEFAULT_REPLICA_SERVER_ID;
		this.replayModeEnabled = DEFAULT_REPLICATION_REPLAY_MODE;
		this.masterRecoveryEnabled = DEFAULT_REPLICATION_MASTER_RECOVERY;

		this.ignoreProducerError = DEFAULT_PRODUCER_IGNORE_ERROR;
		this.producerAckTimeout = DEFAULT_PRODUCER_ACK_TIMEOUT;
		this.producerPartitionKey = DEFAULT_PRODUCER_PARTITION_KEY;
		this.producerType = DEFAULT_PRODUCER_TYPE;
		this.customProducerProperties = new Properties();

		this.metricsPrefix = DEFAULT_METRICS_PREFIX;
		this.metricsJvmEnabled = DEFAULT_METRCS_JVM;

		this.masterRecoveryEnabled = false;
		this.filter = new MaxwellFilter();
		this.outputConfig = new MaxwellOutputConfig();
	}

	public void validate() {
		validatePartitionBy();

		if (!bootstrapperType.equals("async") && !bootstrapperType.equals("sync") && !bootstrapperType.equals("none")) {
			throw new InvalidOptionException("please specify --bootstrapper=async|sync|none", "--bootstrapper");
		}

		if (maxwellMysql.sslMode == null) {
			maxwellMysql.sslMode = SSLMode.DISABLED;
		}

		if (maxwellMysql.host == null) {
			LOGGER.warn("maxwell mysql host not specified, defaulting to localhost");
			maxwellMysql.host = "localhost";
		}

		if (replicationMysql.host == null || replicationMysql.sslMode == null) {
			if (replicationMysql.host != null || replicationMysql.user != null || replicationMysql.password != null) {
				throw new InvalidOptionException("Please specify all of: replication_host, replication_user, replication_password", "--replication");
			}

			replicationMysql = new MaxwellMysqlConfig(maxwellMysql.host, maxwellMysql.port, null, maxwellMysql.user, maxwellMysql.password, maxwellMysql.sslMode);
			replicationMysql.jdbcOptions = maxwellMysql.jdbcOptions;
		}

		if (replicationMysql.sslMode== null) {
			replicationMysql.sslMode = maxwellMysql.sslMode;
		}

		if (gtidMode && masterRecoveryEnabled) {
			throw new InvalidOptionException("There is no need to perform master_recovery under gtid_mode", "--gtid_mode");
		}

		if (outputConfig != null && outputConfig.includesGtidPosition && !gtidMode) {
			throw new InvalidOptionException("output_gtid_position is only support with gtid mode.", "--output_gtid_position");
		}

		if (schemaMysql.host != null) {
			if (schemaMysql.user == null || schemaMysql.password == null) {
				throw new InvalidOptionException("Please specify all of: schema_host, schema_user, schema_password", "--schema");
			}

			if (replicationMysql.host == null) {
				throw new InvalidOptionException("Specifying schema_host only makes sense along with replication_host");
			}
		}

		if (schemaMysql.sslMode == null) {
			schemaMysql.sslMode = maxwellMysql.sslMode;
		}

		if (outputConfig.isEncryptionEnabled() && outputConfig.secretKey == null)
			throw new InvalidUsageException("--secret_key required");

		if (!maxwellMysql.isSameServerAs(replicationMysql) && !bootstrapperType.equals("none")) {
			LOGGER.warn("disabling bootstrapping; not available when using a separate replication host.");
			bootstrapperType = "none";
		}
	}

	private void validatePartitionBy() {
		String[] validPartitionBy = {"database", "table", "primary_key", "column"};
		if (this.producerPartitionKey == null) {
			this.producerPartitionKey = "database";
		} else if (!ArrayUtils.contains(validPartitionBy, this.producerPartitionKey)) {
			throw new InvalidOptionException("please specify --producer_partition_by=database|table|primary_key|column", "producer_partition_by");
		} else if (this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionColumns)) {
			throw new InvalidOptionException("please specify --producer_partition_columns=column1 when using producer_partition_by=column", "producer_partition_columns");
		} else if (this.producerPartitionKey.equals("column") && StringUtils.isEmpty(this.producerPartitionFallback)) {
			throw new InvalidOptionException("please specify --producer_partition_by_fallback=[database, table, primary_key] when using producer_partition_by=column", "producer_partition_by_fallback");
		}

	}

}
