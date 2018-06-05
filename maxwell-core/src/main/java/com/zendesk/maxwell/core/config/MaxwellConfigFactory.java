package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.core.producer.EncryptionMode;
import com.zendesk.maxwell.core.producer.ProducerExtensionConfigurators;
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
	private final ProducerExtensionConfigurators producerExtensionConfigurators;

	@Autowired
	public MaxwellConfigFactory(ConfigurationSupport configurationSupport, ProducerExtensionConfigurators producerExtensionConfigurators) {
		this.configurationSupport = configurationSupport;
		this.producerExtensionConfigurators = producerExtensionConfigurators;
	}

	public MaxwellConfig createNewDefaultConfiguration() {
		return createFor(new Properties());
	}

	public MaxwellConfig createFor(final Properties properties) {
		MaxwellConfig config = new MaxwellConfig();
		config.setLogLevel(configurationSupport.fetchOption("log_level", properties, null));

		config.setMaxwellMysql(configurationSupport.parseMysqlConfig("", properties));
		config.setReplicationMysql(configurationSupport.parseMysqlConfig("replication_", properties));
		config.setSchemaMysql(configurationSupport.parseMysqlConfig("schema_", properties));
		config.setGtidMode(configurationSupport.fetchBooleanOption("gtid_mode", properties, System.getenv(MaxwellConfig.GTID_MODE_ENV) != null));

		config.setDatabaseName(configurationSupport.fetchOption("schema_database", properties, "maxwell"));
		config.getMaxwellMysql().database = config.getDatabaseName();


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
		configureOutputConfig(properties, config);
		configureEncryption(properties, config);
		return config;
	}

	private void configureProducer(final Properties properties, final MaxwellConfig config) {
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

		//configurators should be executed as last step as they might overwrite existing settings.
		if(producerType != null){
			ExtensionConfiguration producerConfig = producerExtensionConfigurators.getByIdentifier(producerType).parseConfiguration(properties).orElse(null);
			config.setProducerConfig(producerConfig);
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

	private void configureDiagnostics(final Properties properties, final MaxwellConfig config) {
		config.setDiagnosticConfig(new MaxwellDiagnosticContext.Config());
		config.getDiagnosticConfig().enable = configurationSupport.fetchBooleanOption("http_diagnostic", properties, false);
		config.getDiagnosticConfig().timeout = configurationSupport.fetchLongOption("http_diagnostic_timeout", properties, 10000L);
	}

	private void configureReplicationSettings(final Properties properties, final MaxwellConfig config) {
		config.setIncludeDatabases(configurationSupport.fetchOption("include_dbs", properties, null));
		config.setExcludeDatabases(configurationSupport.fetchOption("exclude_dbs", properties, null));
		config.setIncludeTables(configurationSupport.fetchOption("include_tables", properties, null));
		config.setExcludeTables(configurationSupport.fetchOption("exclude_tables", properties, null));
		config.setBlacklistDatabases(configurationSupport.fetchOption("blacklist_dbs", properties, null));
		config.setBlacklistTables(configurationSupport.fetchOption("blacklist_tables", properties, null));
		config.setIncludeColumnValues(configurationSupport.fetchOption("include_column_values", properties, null));
		config.setExcludeColumns(configurationSupport.fetchOption("exclude_columns", properties, null));

		if (properties.containsKey("init_position")) {
			String initPosition = properties.getProperty("init_position");
			String[] initPositionSplit = initPosition.split(":");

			if (initPositionSplit.length < 2)
				throw new InvalidOptionException("Invalid init_position: " + initPosition, "--init_position");

			Long pos = 0L;
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

	private void configureOutputConfig(final Properties properties, final MaxwellConfig config) {
		config.getOutputConfig().includesBinlogPosition = configurationSupport.fetchBooleanOption("output_binlog_position", properties, false);
		config.getOutputConfig().includesGtidPosition = configurationSupport.fetchBooleanOption("output_gtid_position", properties, false);
		config.getOutputConfig().includesCommitInfo = configurationSupport.fetchBooleanOption("output_commit_info", properties, true);
		config.getOutputConfig().includesXOffset = configurationSupport.fetchBooleanOption("output_xoffset", properties, true);
		config.getOutputConfig().includesNulls = configurationSupport.fetchBooleanOption("output_nulls", properties, true);
		config.getOutputConfig().includesServerId = configurationSupport.fetchBooleanOption("output_server_id", properties, false);
		config.getOutputConfig().includesThreadId = configurationSupport.fetchBooleanOption("output_thread_id", properties, false);
		config.getOutputConfig().includesRowQuery = configurationSupport.fetchBooleanOption("output_row_query", properties, false);
		config.getOutputConfig().outputDDL = configurationSupport.fetchBooleanOption("output_ddl", properties, false);
	}

	private void configureEncryption(final Properties properties, final MaxwellConfig config) {
		String encryptionMode = configurationSupport.fetchOption("encrypt", properties, "none");
		switch (encryptionMode) {
			case "none":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_NONE;
				break;
			case "data":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_DATA;
				break;
			case "all":
				config.getOutputConfig().encryptionMode = EncryptionMode.ENCRYPT_ALL;
				break;
			default:
				throw new InvalidUsageException("Unknown encryption mode: " + encryptionMode);
		}

		if (config.getOutputConfig().encryptionEnabled()) {
			config.getOutputConfig().secretKey = configurationSupport.fetchOption("secret_key", properties, null);
		}
	}
}
