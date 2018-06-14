package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.*;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Enumeration;
import java.util.Properties;

import static com.zendesk.maxwell.core.config.MaxwellConfig.*;
import static com.zendesk.maxwell.core.config.MaxwellOutputConfig.*;

@Service
public class MaxwellConfigFactory {
	private final ConfigurationSupport configurationSupport;
	private final MySqlConfigurationSupport mySqlConfigurationSupport;

	@Autowired
	public MaxwellConfigFactory(ConfigurationSupport configurationSupport, MySqlConfigurationSupport mySqlConfigurationSupport) {
		this.configurationSupport = configurationSupport;
		this.mySqlConfigurationSupport = mySqlConfigurationSupport;
	}

	public MaxwellConfig create() {
		return createFor(new Properties());
	}

	public MaxwellConfig createFor(final Properties properties) {
		MaxwellConfig config = new MaxwellConfig();
		config.logLevel = configurationSupport.fetchOption("log_level", properties, null);

		config.maxwellMysql = mySqlConfigurationSupport.parseMysqlConfig("", properties);
		config.replicationMysql = mySqlConfigurationSupport.parseMysqlConfig("replication_", properties);
		config.schemaMysql = mySqlConfigurationSupport.parseMysqlConfig("schema_", properties);
		config.gtidMode = configurationSupport.fetchBooleanOption("gtid_mode", properties, System.getenv(GTID_MODE_ENV) != null);

		config.databaseName = configurationSupport.fetchOption("schema_database", properties, DEFAULT_DATABASE_NAME);
		config.maxwellMysql.database = config.databaseName;

		configureProducer(properties, config);

		config.bootstrapperType = configurationSupport.fetchOption("bootstrapper", properties, DEFAULT_BOOTSTRAPPER_TYPE);
		config.clientID = configurationSupport.fetchOption("client_id", properties, DEFAULT_CLIENT_ID);
		config.replicaServerID = configurationSupport.fetchLongOption("replica_server_id", properties, DEFAULT_REPLICA_SERVER_ID);

		config.metricsPrefix = configurationSupport.fetchOption("metrics_prefix", properties, DEFAULT_METRICS_PREFIX);
		config.metricsReportingType = configurationSupport.fetchOption("metrics_type", properties, null);
		config.metricsJvmEnabled = configurationSupport.fetchBooleanOption("metrics_jvm", properties, DEFAULT_METRCS_JVM);

		configureReplicationSettings(properties, config);
		configureFilter(properties, config);
		configureOutputConfig(properties, config);
		return config;
	}

	private void configureProducer(final Properties properties, final MaxwellConfig config) {
		config.ignoreProducerError = configurationSupport.fetchBooleanOption("ignore_producer_error", properties, DEFAULT_PRODUCER_IGNORE_ERROR);
		config.producerAckTimeout = configurationSupport.fetchLongOption("producer_ack_timeout", properties, DEFAULT_PRODUCER_ACK_TIMEOUT);
		config.producerPartitionKey = configurationSupport.fetchOption("producer_partition_by", properties, DEFAULT_PRODUCER_PARTITION_KEY);
		config.producerPartitionColumns = configurationSupport.fetchOption("producer_partition_columns", properties, null);
		config.producerPartitionFallback = configurationSupport.fetchOption("producer_partition_by_fallback", properties, null);
		config.producerFactory = configurationSupport.fetchOption("custom_producer.factory", properties, null);

		String producerType = configurationSupport.fetchOption("producer", properties, DEFAULT_PRODUCER_TYPE);
		config.producerType = producerType;
		if (properties != null) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("custom_producer.")) {
					config.customProducerProperties.setProperty(k.replace("custom_producer.", ""), properties.getProperty(k));
				}
			}
		}
	}

	private void configureReplicationSettings(final Properties properties, final MaxwellConfig config) {
		if (properties.containsKey("init_position")) {
			String initPosition = properties.getProperty("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if (initPositionSplit.length < 2)
				throw new InvalidOptionException("Invalid init_position: " + initPosition, "--init_position");

			Long pos;
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

			config.initPosition = (new Position(new BinlogPosition(pos, initPositionSplit[0]), lastHeartbeat));
		}

		config.replayModeEnabled = configurationSupport.fetchBooleanOption("replay", properties, DEFAULT_REPLICATION_REPLAY_MODE);
		config.masterRecoveryEnabled = configurationSupport.fetchBooleanOption("master_recovery", properties, DEFAULT_REPLICATION_MASTER_RECOVERY);
	}

	private void configureFilter(Properties properties, MaxwellConfig config) {
		try {
			String includeDatabases = configurationSupport.fetchOption("include_dbs", properties, null);
			String excludeDatabases = configurationSupport.fetchOption("exclude_dbs", properties, null);
			String includeTables = configurationSupport.fetchOption("include_tables", properties, null);
			String excludeTables = configurationSupport.fetchOption("exclude_tables", properties, null);
			String blacklistDatabases = configurationSupport.fetchOption("blacklist_dbs", properties, null);
			String blacklistTables = configurationSupport.fetchOption("blacklist_tables", properties, null);
			String includeColumnValues = configurationSupport.fetchOption("include_column_values", properties, null);
			config.filter = new MaxwellFilter(includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables, includeColumnValues);
		} catch (MaxwellInvalidFilterException e) {
			throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	private void configureOutputConfig(final Properties properties, final MaxwellConfig config) {
		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.includesBinlogPosition = configurationSupport.fetchBooleanOption("output_binlog_position", properties, DEFAULT_INCLUDE_BINLOG_POSITION);
		outputConfig.includesGtidPosition = configurationSupport.fetchBooleanOption("output_gtid_position", properties, DEFAULT_INCLUDE_GTID_POSITION);
		outputConfig.includesCommitInfo = configurationSupport.fetchBooleanOption("output_commit_info", properties, DEFAULT_INCLUDE_COMMIT_INFO);
		outputConfig.includesXOffset = configurationSupport.fetchBooleanOption("output_xoffset", properties, DEFAULT_INCLUDE_XOFFSET);
		outputConfig.includesNulls = configurationSupport.fetchBooleanOption("output_nulls", properties, DEFAULT_INCLUDE_NULLS);
		outputConfig.includesServerId = configurationSupport.fetchBooleanOption("output_server_id", properties, DEFAULT_INCLUDE_SERVER_ID);
		outputConfig.includesThreadId = configurationSupport.fetchBooleanOption("output_thread_id", properties, DEFAULT_INCLUDE_THREAD_ID);
		outputConfig.includesRowQuery = configurationSupport.fetchBooleanOption("output_row_query", properties, DEFAULT_INCLUDE_ROW_QUERY);
		outputConfig.outputDDL = configurationSupport.fetchBooleanOption("output_ddl", properties, DEFAULT_OUTPUT_DDL);
		String encryptionMode = configurationSupport.fetchOption("encrypt", properties, DEFAULT_ENCRYPTION_MODE);
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
				throw new InvalidUsageException("Unknown encryption mode: " + encryptionMode);
		}

		if (outputConfig.isEncryptionEnabled()) {
			outputConfig.secretKey = configurationSupport.fetchOption("secret_key", properties, null);
		}

		String excludeColumns = configurationSupport.fetchOption("exclude_columns", properties, null);
		if (excludeColumns != null) {
			for (String s : excludeColumns.split(",")) {
				try {
					outputConfig.excludeColumns.add(MaxwellFilterSupport.compileStringToPattern(s));
				} catch (MaxwellInvalidFilterException e) {
					throw new InvalidUsageException("invalid exclude_columns: '" + excludeColumns + "': " + e.getMessage());
				}
			}
		}
		config.outputConfig =  outputConfig;
	}
}
