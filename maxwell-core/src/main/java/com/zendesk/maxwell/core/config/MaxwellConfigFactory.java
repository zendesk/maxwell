package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.config.*;
import com.zendesk.maxwell.api.producer.EncryptionMode;
import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Enumeration;
import java.util.Properties;

@Service
public class MaxwellConfigFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellConfigFactory.class);

	private final ConfigurationSupport configurationSupport;
	private final MySqlConfigurationSupport mySqlConfigurationSupport;

	@Autowired
	public MaxwellConfigFactory(ConfigurationSupport configurationSupport, MySqlConfigurationSupport mySqlConfigurationSupport) {
		this.configurationSupport = configurationSupport;
		this.mySqlConfigurationSupport = mySqlConfigurationSupport;
	}

	public BaseMaxwellConfig create() {
		return new BaseMaxwellConfig();
	}

	public BaseMaxwellConfig createFor(final Properties properties) {
		BaseMaxwellConfig config = new BaseMaxwellConfig();
		config.setLogLevel(configurationSupport.fetchOption("log_level", properties, null));

		config.setMaxwellMysql(mySqlConfigurationSupport.parseMysqlConfig("", properties));
		config.setReplicationMysql(mySqlConfigurationSupport.parseMysqlConfig("replication_", properties));
		config.setSchemaMysql(mySqlConfigurationSupport.parseMysqlConfig("schema_", properties));
		config.setGtidMode(configurationSupport.fetchBooleanOption("gtid_mode", properties, System.getenv(MaxwellConfig.GTID_MODE_ENV) != null));

		config.setDatabaseName(configurationSupport.fetchOption("schema_database", properties, MaxwellConfig.DEFAULT_DATABASE_NAME));
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setDatabase(config.getDatabaseName());

		configureProducer(properties, config);

		config.setBootstrapperType(configurationSupport.fetchOption("bootstrapper", properties, MaxwellConfig.DEFAULT_BOOTSTRAPPER_TYPE));
		config.setClientID(configurationSupport.fetchOption("client_id", properties, MaxwellConfig.DEFAULT_CLIENT_ID));
		config.setReplicaServerID(configurationSupport.fetchLongOption("replica_server_id", properties, MaxwellConfig.DEFAULT_REPLICA_SERVER_ID));


		config.setMetricsPrefix(configurationSupport.fetchOption("metrics_prefix", properties, MaxwellConfig.DEFAULT_METRICS_PREFIX));
		config.setMetricsReportingType(configurationSupport.fetchOption("metrics_type", properties, null));
		config.setMetricsSlf4jInterval(configurationSupport.fetchLongOption("metrics_slf4j_interval", properties, MaxwellConfig.DEFAULT_METRITCS_SLF4J_INTERVAL));

		// TODO remove metrics_http_port support once hitting v1.11.x
		String metricsHttpPort = configurationSupport.fetchOption("metrics_http_port", properties, null);
		if (metricsHttpPort != null) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			config.setHttpPort(Integer.parseInt(metricsHttpPort));
		} else {
			config.setHttpPort(configurationSupport.fetchIntegerOption("http_port", properties, MaxwellConfig.DEFAULT_HTTP_PORT));
		}
		config.setHttpBindAddress(configurationSupport.fetchOption("http_bind_address", properties, null));
		config.setHttpPathPrefix(configurationSupport.fetchOption("http_path_prefix", properties, MaxwellConfig.DEFAULT_HTTP_PATH_PREFIX));

		if (!config.getHttpPathPrefix().startsWith("/")) {
			config.setHttpPathPrefix("/" + config.getHttpPathPrefix());
		}
		config.setMetricsDatadogType(configurationSupport.fetchOption("metrics_datadog_type", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_TYPE));
		config.setMetricsDatadogTags(configurationSupport.fetchOption("metrics_datadog_tags", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_TAGS));
		config.setMetricsDatadogAPIKey(configurationSupport.fetchOption("metrics_datadog_apikey", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_APIKEY));
		config.setMetricsDatadogHost(configurationSupport.fetchOption("metrics_datadog_host", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_HOST));
		config.setMetricsDatadogPort(configurationSupport.fetchIntegerOption("metrics_datadog_port", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_PORT));
		config.setMetricsDatadogInterval(configurationSupport.fetchLongOption("metrics_datadog_interval", properties, MaxwellConfig.DEFAULT_METRICS_DATADOG_INTERVAL));

		config.setMetricsJvm(configurationSupport.fetchBooleanOption("metrics_jvm", properties, MaxwellConfig.DEFAULT_METRCS_JVM));

		configureDiagnostics(properties, config);

		configureReplicationSettings(properties, config);
		configureFilter(properties, config);
		configureOutputConfig(properties, config);
		return config;
	}

	private void configureProducer(final Properties properties, final BaseMaxwellConfig config) {
		config.setIgnoreProducerError(configurationSupport.fetchBooleanOption("ignore_producer_error", properties, MaxwellConfig.DEFAULT_PRODUCER_IGNORE_ERROR));
		config.setProducerAckTimeout(configurationSupport.fetchLongOption("producer_ack_timeout", properties, MaxwellConfig.DEFAULT_PRODUCER_ACK_TIMEOUT));
		config.setProducerPartitionKey(configurationSupport.fetchOption("producer_partition_by", properties, MaxwellConfig.DEFAULT_PRODUCER_PARTITION_KEY));
		config.setProducerPartitionColumns(configurationSupport.fetchOption("producer_partition_columns", properties, null));
		config.setProducerPartitionFallback(configurationSupport.fetchOption("producer_partition_by_fallback", properties, null));
		config.setProducerFactory(configurationSupport.fetchOption("custom_producer.factory", properties, null));

		String producerType = configurationSupport.fetchOption("producer", properties, MaxwellConfig.DEFAULT_PRODUCER_TYPE);
		config.setProducerType(producerType);
		if (properties != null) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith("custom_producer.")) {
					config.getCustomProducerProperties().setProperty(k.replace("custom_producer.", ""), properties.getProperty(k));
				}
			}
		}
	}

	private void configureDiagnostics(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellDiagnosticConfig diagnosticConfig = new BaseMaxwellDiagnosticConfig();
		diagnosticConfig.setEnable(configurationSupport.fetchBooleanOption("http_diagnostic", properties, MaxwellDiagnosticConfig.DEFAULT_DIAGNOSTIC_HTTP));
		diagnosticConfig.setTimeout(configurationSupport.fetchLongOption("http_diagnostic_timeout", properties, MaxwellDiagnosticConfig.DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT));
		config.setDiagnosticConfig(diagnosticConfig);
	}

	private void configureReplicationSettings(final Properties properties, final BaseMaxwellConfig config) {
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

			config.setInitPosition(new Position(new BinlogPosition(pos, initPositionSplit[0]), lastHeartbeat));
		}

		config.setReplayMode(configurationSupport.fetchBooleanOption("replay", properties, MaxwellConfig.DEFAULT_REPLICATION_REPLAY_MODE));
		config.setMasterRecovery(configurationSupport.fetchBooleanOption("master_recovery", properties, MaxwellConfig.DEFAULT_REPLICATION_MASTER_RECOVERY));
	}

	private void configureFilter(Properties properties, BaseMaxwellConfig config) {
		try {
			String includeDatabases = configurationSupport.fetchOption("include_dbs", properties, null);
			String excludeDatabases = configurationSupport.fetchOption("exclude_dbs", properties, null);
			String includeTables = configurationSupport.fetchOption("include_tables", properties, null);
			String excludeTables = configurationSupport.fetchOption("exclude_tables", properties, null);
			String blacklistDatabases = configurationSupport.fetchOption("blacklist_dbs", properties, null);
			String blacklistTables = configurationSupport.fetchOption("blacklist_tables", properties, null);
			String includeColumnValues = configurationSupport.fetchOption("include_column_values", properties, null);
			config.setFilter(new BaseMaxwellFilter(includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables, includeColumnValues));
		} catch (MaxwellInvalidFilterException e) {
			throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	private void configureOutputConfig(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellOutputConfig outputConfig = new BaseMaxwellOutputConfig();
		outputConfig.setIncludesBinlogPosition(configurationSupport.fetchBooleanOption("output_binlog_position", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_BINLOG_POSITION));
		outputConfig.setIncludesGtidPosition(configurationSupport.fetchBooleanOption("output_gtid_position", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_GTID_POSITION));
		outputConfig.setIncludesCommitInfo(configurationSupport.fetchBooleanOption("output_commit_info", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_COMMIT_INFO));
		outputConfig.setIncludesXOffset(configurationSupport.fetchBooleanOption("output_xoffset", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_XOFFSET));
		outputConfig.setIncludesNulls(configurationSupport.fetchBooleanOption("output_nulls", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_NULLS));
		outputConfig.setIncludesServerId(configurationSupport.fetchBooleanOption("output_server_id", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_SERVER_ID));
		outputConfig.setIncludesThreadId(configurationSupport.fetchBooleanOption("output_thread_id", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_THREAD_ID));
		outputConfig.setIncludesRowQuery(configurationSupport.fetchBooleanOption("output_row_query", properties, MaxwellOutputConfig.DEFAULT_INCLUDE_ROW_QUERY));
		outputConfig.setOutputDDL(configurationSupport.fetchBooleanOption("output_ddl", properties, MaxwellOutputConfig.DEFAULT_OUTPUT_DDL));
		String encryptionMode = configurationSupport.fetchOption("encrypt", properties, MaxwellOutputConfig.DEFAULT_ENCRYPTION_MODE);
		switch (encryptionMode) {
			case "none":
				outputConfig.setEncryptionMode(EncryptionMode.ENCRYPT_NONE);
				break;
			case "data":
				outputConfig.setEncryptionMode(EncryptionMode.ENCRYPT_DATA);
				break;
			case "all":
				outputConfig.setEncryptionMode(EncryptionMode.ENCRYPT_ALL);
				break;
			default:
				throw new InvalidUsageException("Unknown encryption mode: " + encryptionMode);
		}

		if (outputConfig.isEncryptionEnabled()) {
			outputConfig.setSecretKey(configurationSupport.fetchOption("secret_key", properties, null));
		}

		String excludeColumns = configurationSupport.fetchOption("exclude_columns", properties, null);
		if (excludeColumns != null) {
			for (String s : excludeColumns.split(",")) {
				try {
					outputConfig.getExcludeColumns().add(MaxwellFilterSupport.compileStringToPattern(s));
				} catch (MaxwellInvalidFilterException e) {
					throw new InvalidUsageException("invalid exclude_columns: '" + excludeColumns + "': " + e.getMessage());
				}
			}
		}
		config.setOutputConfig(outputConfig);
	}
}
