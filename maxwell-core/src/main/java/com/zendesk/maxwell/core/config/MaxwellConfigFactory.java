package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.producer.EncryptionMode;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
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

	@Autowired
	public MaxwellConfigFactory(ConfigurationSupport configurationSupport) {
		this.configurationSupport = configurationSupport;
	}

	public BaseMaxwellConfig createNewDefaultConfiguration() {
		return createFor(new Properties());
	}

	public BaseMaxwellConfig createFor(final Properties properties) {
		BaseMaxwellConfig config = new BaseMaxwellConfig();
		config.setLogLevel(configurationSupport.fetchOption("log_level", properties, null));

		config.setMaxwellMysql(configurationSupport.parseMysqlConfig("", properties));
		config.setReplicationMysql(configurationSupport.parseMysqlConfig("replication_", properties));
		config.setSchemaMysql(configurationSupport.parseMysqlConfig("schema_", properties));
		config.setGtidMode(configurationSupport.fetchBooleanOption("gtid_mode", properties, System.getenv(MaxwellConfig.GTID_MODE_ENV) != null));

		config.setDatabaseName(configurationSupport.fetchOption("schema_database", properties, "maxwell"));
		((BaseMaxwellMysqlConfig)config.getMaxwellMysql()).setDatabase(config.getDatabaseName());

		configureProducer(properties, config);

		config.setBootstrapperType(configurationSupport.fetchOption("bootstrapper", properties, "async"));
		config.setClientID(configurationSupport.fetchOption("client_id", properties, "maxwell"));
		config.setReplicaServerID(configurationSupport.fetchLongOption("replica_server_id", properties, 6379L));


		config.setMetricsPrefix(configurationSupport.fetchOption("metrics_prefix", properties, "MaxwellMetrics"));
		config.setMetricsReportingType(configurationSupport.fetchOption("metrics_type", properties, null));
		config.setMetricsSlf4jInterval(configurationSupport.fetchLongOption("metrics_slf4j_interval", properties, 60L));

		// TODO remove metrics_http_port support once hitting v1.11.x
		String metricsHttpPort = configurationSupport.fetchOption("metrics_http_port", properties, null);
		if (metricsHttpPort != null) {
			LOGGER.warn("metrics_http_port is deprecated, please use http_port");
			config.setHttpPort(Integer.parseInt(metricsHttpPort));
		} else {
			config.setHttpPort(Integer.parseInt(configurationSupport.fetchOption("http_port", properties, "8080")));
		}
		config.setHttpBindAddress(configurationSupport.fetchOption("http_bind_address", properties, null));
		config.setHttpPathPrefix(configurationSupport.fetchOption("http_path_prefix", properties, "/"));

		if (!config.getHttpPathPrefix().startsWith("/")) {
			config.setHttpPathPrefix("/" + config.getHttpPathPrefix());
		}
		config.setMetricsDatadogType(configurationSupport.fetchOption("metrics_datadog_type", properties, "udp"));
		config.setMetricsDatadogTags(configurationSupport.fetchOption("metrics_datadog_tags", properties, ""));
		config.setMetricsDatadogAPIKey(configurationSupport.fetchOption("metrics_datadog_apikey", properties, ""));
		config.setMetricsDatadogHost(configurationSupport.fetchOption("metrics_datadog_host", properties, "localhost"));
		config.setMetricsDatadogPort(Integer.parseInt(configurationSupport.fetchOption("metrics_datadog_port", properties, "8125")));
		config.setMetricsDatadogInterval(configurationSupport.fetchLongOption("metrics_datadog_interval", properties, 60L));

		config.setMetricsJvm(configurationSupport.fetchBooleanOption("metrics_jvm", properties, false));

		configureDiagnostics(properties, config);

		configureReplicationSettings(properties, config);
		configureFilter(properties, config);
		configureOutputConfig(properties, config);
		return config;
	}

	private void configureProducer(final Properties properties, final BaseMaxwellConfig config) {
		config.setProducerAckTimeout(configurationSupport.fetchLongOption("producer_ack_timeout", properties, 0L));
		config.setProducerPartitionKey(configurationSupport.fetchOption("producer_partition_by", properties, "database"));
		config.setProducerPartitionColumns(configurationSupport.fetchOption("producer_partition_columns", properties, null));
		config.setProducerPartitionFallback(configurationSupport.fetchOption("producer_partition_by_fallback", properties, null));
		config.setProducerFactory(fetchProducerFactory(properties));
		String producerType = configurationSupport.fetchOption("producer", properties, "stdout");
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

	private ProducerFactory fetchProducerFactory(final Properties properties) {
		String name = "custom_producer.factory";
		String strOption = configurationSupport.fetchOption(name, properties, null);
		if (strOption != null) {
			try {
				Class<?> clazz = Class.forName(strOption);
				return ProducerFactory.class.cast(clazz.newInstance());
			} catch (ClassNotFoundException e) {
				throw new InvalidOptionException("Invalid value for " + name + ", class not found", "--" + name);
			} catch (IllegalAccessException | InstantiationException | ClassCastException e) {
				throw new InvalidOptionException("Invalid value for " + name + ", class instantiation error", "--" + name);
			}
		} else {
			return null;
		}
	}

	private void configureDiagnostics(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellDiagnosticConfig diagnosticConfig = new BaseMaxwellDiagnosticConfig();
		diagnosticConfig.setEnable(configurationSupport.fetchBooleanOption("http_diagnostic", properties, false));
		diagnosticConfig.setTimeout(configurationSupport.fetchLongOption("http_diagnostic_timeout", properties, 10000L));
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

		config.setReplayMode(configurationSupport.fetchBooleanOption("replay", properties, false));
		config.setMasterRecovery(configurationSupport.fetchBooleanOption("master_recovery", properties, false));
		config.setIgnoreProducerError(configurationSupport.fetchBooleanOption("ignore_producer_error", properties, true));
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
			config.setFilter(new MaxwellFilter(includeDatabases, excludeDatabases, includeTables, excludeTables, blacklistDatabases, blacklistTables, includeColumnValues));
		} catch (MaxwellInvalidFilterException e) {
			throw new InvalidUsageException("Invalid filter options: " + e.getLocalizedMessage());
		}
	}

	private void configureOutputConfig(final Properties properties, final BaseMaxwellConfig config) {
		BaseMaxwellOutputConfig outputConfig = new BaseMaxwellOutputConfig();
		outputConfig.setIncludesBinlogPosition(configurationSupport.fetchBooleanOption("output_binlog_position", properties, false));
		outputConfig.setIncludesGtidPosition(configurationSupport.fetchBooleanOption("output_gtid_position", properties, false));
		outputConfig.setIncludesCommitInfo(configurationSupport.fetchBooleanOption("output_commit_info", properties, true));
		outputConfig.setIncludesXOffset(configurationSupport.fetchBooleanOption("output_xoffset", properties, true));
		outputConfig.setIncludesNulls(configurationSupport.fetchBooleanOption("output_nulls", properties, true));
		outputConfig.setIncludesServerId(configurationSupport.fetchBooleanOption("output_server_id", properties, false));
		outputConfig.setIncludesThreadId(configurationSupport.fetchBooleanOption("output_thread_id", properties, false));
		outputConfig.setIncludesRowQuery(configurationSupport.fetchBooleanOption("output_row_query", properties, false));
		outputConfig.setOutputDDL(configurationSupport.fetchBooleanOption("output_ddl", properties, false));
		String encryptionMode = configurationSupport.fetchOption("encrypt", properties, "none");
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
					outputConfig.getExcludeColumns().add(MaxwellConfig.compileStringToPattern(s));
				} catch (MaxwellInvalidFilterException e) {
					throw new InvalidUsageException("invalid exclude_columns: '" + excludeColumns + "': " + e.getMessage());
				}
			}
		}
		config.setOutputConfig(outputConfig);
	}
}
