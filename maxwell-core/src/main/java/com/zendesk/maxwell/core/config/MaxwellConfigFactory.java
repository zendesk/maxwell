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

import static com.zendesk.maxwell.api.config.MaxwellConfig.*;
import static com.zendesk.maxwell.api.config.MaxwellOutputConfig.*;
import static com.zendesk.maxwell.api.config.MaxwellFilter.*;
import static com.zendesk.maxwell.api.config.MaxwellMysqlConfig.*;

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
		return createFor(new Properties());
	}

	public BaseMaxwellConfig createFor(final Properties properties) {
		BaseMaxwellConfig config = new BaseMaxwellConfig();
		config.setLogLevel(configurationSupport.fetchOption(CONFIGURATION_OPTION_LOG_LEVEL, properties, null));

		config.setMaxwellMysql(mySqlConfigurationSupport.parseMysqlConfig("", properties));
		config.setReplicationMysql(mySqlConfigurationSupport.parseMysqlConfig(CONFIGURATION_OPTION_PREFIX_REPLICATION, properties));
		config.setSchemaMysql(mySqlConfigurationSupport.parseMysqlConfig(CONFIGURATION_OPTION_PREFIX_SCHEMA, properties));
		config.setGtidMode(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_GTID_MODE, properties, System.getenv(GTID_MODE_ENV) != null));

		config.setDatabaseName(configurationSupport.fetchOption(CONFIGURATION_OPTION_SCHEMA_DATABASE, properties, DEFAULT_DATABASE_NAME));
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setDatabase(config.getDatabaseName());

		configureProducer(properties, config);

		config.setBootstrapperType(configurationSupport.fetchOption(CONFIGURATION_OPTION_BOOTSTRAPPER, properties, DEFAULT_BOOTSTRAPPER_TYPE));
		config.setClientID(configurationSupport.fetchOption(CONFIGURATION_OPTION_CLIENT_ID, properties, DEFAULT_CLIENT_ID));
		config.setReplicaServerID(configurationSupport.fetchLongOption(CONFIGURATION_OPTION_REPLICA_SERVER_ID, properties, DEFAULT_REPLICA_SERVER_ID));


		config.setMetricsPrefix(configurationSupport.fetchOption(CONFIGURATION_OPTION_METRICS_PREFIX, properties, DEFAULT_METRICS_PREFIX));
		config.setMetricsReportingType(configurationSupport.fetchOption(CONFIGURATION_OPTION_METRICS_TYPE, properties, null));

		// TODO remove metrics_http_port support once hitting v1.11.x
		String metricsHttpPort = configurationSupport.fetchOption(CONFIGURATION_OPTION_METRICS_HTTP_PORT, properties, null);
		if (metricsHttpPort != null) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			config.setHttpPort(Integer.parseInt(metricsHttpPort));
		} else {
			config.setHttpPort(configurationSupport.fetchIntegerOption(CONFIGURATION_OPTION_HTTP_PORT, properties, DEFAULT_HTTP_PORT));
		}
		config.setHttpBindAddress(configurationSupport.fetchOption(CONFIGURATION_OPTION_HTTP_BIND_ADDRESS, properties, null));
		config.setHttpPathPrefix(configurationSupport.fetchOption(CONFIGURATION_OPTION_HTTP_PATH_PREFIX, properties, DEFAULT_HTTP_PATH_PREFIX));

		if (!config.getHttpPathPrefix().startsWith("/")) {
			config.setHttpPathPrefix("/" + config.getHttpPathPrefix());
		}
		config.setMetricsDatadogType(configurationSupport.fetchOption("metrics_datadog_type", properties, DEFAULT_METRICS_DATADOG_TYPE));
		config.setMetricsDatadogTags(configurationSupport.fetchOption("metrics_datadog_tags", properties, DEFAULT_METRICS_DATADOG_TAGS));
		config.setMetricsDatadogAPIKey(configurationSupport.fetchOption("metrics_datadog_apikey", properties, DEFAULT_METRICS_DATADOG_APIKEY));
		config.setMetricsDatadogHost(configurationSupport.fetchOption("metrics_datadog_host", properties, DEFAULT_METRICS_DATADOG_HOST));
		config.setMetricsDatadogPort(configurationSupport.fetchIntegerOption("metrics_datadog_port", properties, DEFAULT_METRICS_DATADOG_PORT));
		config.setMetricsDatadogInterval(configurationSupport.fetchLongOption("metrics_datadog_interval", properties, DEFAULT_METRICS_DATADOG_INTERVAL));

		config.setMetricsJvm(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_METRICS_JVM, properties, DEFAULT_METRCS_JVM));

		configureDiagnostics(properties, config);

		configureReplicationSettings(properties, config);
		configureFilter(properties, config);
		configureOutputConfig(properties, config);
		return config;
	}

	private void configureProducer(final Properties properties, final BaseMaxwellConfig config) {
		config.setIgnoreProducerError(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_IGNORE_PRODUCER_ERROR, properties, DEFAULT_PRODUCER_IGNORE_ERROR));
		config.setProducerAckTimeout(configurationSupport.fetchLongOption(CONFIGURATION_OPTION_PRODUCER_ACK_TIMEOUT, properties, DEFAULT_PRODUCER_ACK_TIMEOUT));
		config.setProducerPartitionKey(configurationSupport.fetchOption(CONFIGURATION_OPTION_PRODUCER_PARTITION_BY, properties, DEFAULT_PRODUCER_PARTITION_KEY));
		config.setProducerPartitionColumns(configurationSupport.fetchOption(CONFIGURATION_OPTION_PRODUCER_PARTITION_COLUMNS, properties, null));
		config.setProducerPartitionFallback(configurationSupport.fetchOption(CONFIGURATION_OPTION_PRODUCER_PARTITION_BY_FALLBACK, properties, null));
		config.setProducerFactory(configurationSupport.fetchOption(CONFIGURATION_OPTION_CUSTOM_PRODUCER_FACTORY, properties, null));

		String producerType = configurationSupport.fetchOption(CONFIGURATION_OPTION_PRODUCER, properties, DEFAULT_PRODUCER_TYPE);
		config.setProducerType(producerType);
		if (properties != null) {
			for (Enumeration<Object> e = properties.keys(); e.hasMoreElements(); ) {
				String k = (String) e.nextElement();
				if (k.startsWith(CONFIGURATION_OPTION_CUSTOM_PRODUCER_CONFIG_PREFIX)) {
					config.getCustomProducerProperties().setProperty(k.replace(CONFIGURATION_OPTION_CUSTOM_PRODUCER_CONFIG_PREFIX, ""), properties.getProperty(k));
				}
			}
		}
	}

	private void configureDiagnostics(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellDiagnosticConfig diagnosticConfig = new BaseMaxwellDiagnosticConfig();
		diagnosticConfig.setEnable(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_HTTP_DIAGNOSTIC, properties, MaxwellDiagnosticConfig.DEFAULT_DIAGNOSTIC_HTTP));
		diagnosticConfig.setTimeout(configurationSupport.fetchLongOption(CONFIGURATION_OPTION_HTTP_DIAGNOSTIC_TIMEOUT, properties, MaxwellDiagnosticConfig.DEFAULT_DIAGNOSTIC_HTTP_TIMEOUT));
		config.setDiagnosticConfig(diagnosticConfig);
	}

	private void configureReplicationSettings(final Properties properties, final BaseMaxwellConfig config) {
		if (properties.containsKey(CONFIGURATION_OPTION_INIT_POSITION)) {
			String initPosition = properties.getProperty(CONFIGURATION_OPTION_INIT_POSITION);
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

		config.setReplayMode(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_REPLAY, properties, DEFAULT_REPLICATION_REPLAY_MODE));
		config.setMasterRecovery(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_MASTER_RECOVERY, properties, DEFAULT_REPLICATION_MASTER_RECOVERY));
	}

	private void configureFilter(Properties properties, BaseMaxwellConfig config) {
		try {
			String includeDatabases = configurationSupport.fetchOption(CONFIGURATION_OPTION_INCLUDE_DBS, properties, null);
			String excludeDatabases = configurationSupport.fetchOption(CONFIGURATION_OPTION_EXCLUDE_DBS, properties, null);
			String includeTables = configurationSupport.fetchOption(CONFIGURATION_OPTION_INCLUDE_TABLES, properties, null);
			String excludeTables = configurationSupport.fetchOption(CONFIGURATION_OPTION_EXCLUDE_TABLES, properties, null);
			String blacklistDatabases = configurationSupport.fetchOption(CONFIGURATION_OPTION_BLACKLIST_DBS, properties, null);
			String blacklistTables = configurationSupport.fetchOption(CONFIGURATION_OPTION_BLACKLIST_TABLES, properties, null);
			String includeColumnValues = configurationSupport.fetchOption(CONFIGURATION_OPTION_INCLUDE_COLUMN_VALUES, properties, null);
			config.setFilter(new BaseMaxwellFilter(includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables, includeColumnValues));
		} catch (MaxwellInvalidFilterException e) {
			throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	private void configureOutputConfig(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellOutputConfig outputConfig = new BaseMaxwellOutputConfig();
		outputConfig.setIncludesBinlogPosition(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_BINLOG_POSITION, properties, DEFAULT_INCLUDE_BINLOG_POSITION));
		outputConfig.setIncludesGtidPosition(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_GTID_POSITION, properties, DEFAULT_INCLUDE_GTID_POSITION));
		outputConfig.setIncludesCommitInfo(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_COMMIT_INFO, properties, DEFAULT_INCLUDE_COMMIT_INFO));
		outputConfig.setIncludesXOffset(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_XOFFSET, properties, DEFAULT_INCLUDE_XOFFSET));
		outputConfig.setIncludesNulls(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_NULLS, properties, DEFAULT_INCLUDE_NULLS));
		outputConfig.setIncludesServerId(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_SERVER_ID, properties, DEFAULT_INCLUDE_SERVER_ID));
		outputConfig.setIncludesThreadId(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_THREAD_ID, properties, DEFAULT_INCLUDE_THREAD_ID));
		outputConfig.setIncludesRowQuery(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_ROW_QUERY, properties, DEFAULT_INCLUDE_ROW_QUERY));
		outputConfig.setOutputDDL(configurationSupport.fetchBooleanOption(CONFIGURATION_OPTION_OUTPUT_DDL, properties, DEFAULT_OUTPUT_DDL));
		String encryptionMode = configurationSupport.fetchOption(CONFIGURATION_OPTION_ENCRYPT, properties, DEFAULT_ENCRYPTION_MODE);
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
