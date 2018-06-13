package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.api.replication.Position;

import java.util.Properties;

public interface MaxwellConfig {

	String CONFIGURATION_OPTION_LOG_LEVEL = "log_level";
	String CONFIGURATION_OPTION_GTID_MODE = "gtid_mode";
	String CONFIGURATION_OPTION_SCHEMA_DATABASE = "schema_database";
	String CONFIGURATION_OPTION_BOOTSTRAPPER = "bootstrapper";
	String CONFIGURATION_OPTION_CLIENT_ID = "client_id";
	String CONFIGURATION_OPTION_REPLICA_SERVER_ID = "replica_server_id";
	String CONFIGURATION_OPTION_METRICS_PREFIX = "metrics_prefix";
	String CONFIGURATION_OPTION_METRICS_TYPE = "metrics_type";
	String CONFIGURATION_OPTION_METRICS_JVM = "metrics_jvm";
	String CONFIGURATION_OPTION_IGNORE_PRODUCER_ERROR = "ignore_producer_error";
	String CONFIGURATION_OPTION_PRODUCER_ACK_TIMEOUT = "producer_ack_timeout";
	String CONFIGURATION_OPTION_PRODUCER_PARTITION_BY = "producer_partition_by";
	String CONFIGURATION_OPTION_PRODUCER_PARTITION_COLUMNS = "producer_partition_columns";
	String CONFIGURATION_OPTION_PRODUCER_PARTITION_BY_FALLBACK = "producer_partition_by_fallback";
	String CONFIGURATION_OPTION_CUSTOM_PRODUCER_FACTORY = "custom_producer.factory";
	String CONFIGURATION_OPTION_PRODUCER = "producer";
	String CONFIGURATION_OPTION_CUSTOM_PRODUCER_CONFIG_PREFIX = "custom_producer.";
	String CONFIGURATION_OPTION_INIT_POSITION = "init_position";
	String CONFIGURATION_OPTION_REPLAY = "replay";
	String CONFIGURATION_OPTION_MASTER_RECOVERY = "master_recovery";


	String DEFAULT_DATABASE_NAME = "maxwell";
	String DEFAULT_BOOTSTRAPPER_TYPE = "async";
	String DEFAULT_CLIENT_ID = "maxwell";

	long DEFAULT_REPLICA_SERVER_ID = 6379L;
	boolean DEFAULT_REPLICATION_REPLAY_MODE = false;
	boolean DEFAULT_REPLICATION_MASTER_RECOVERY = false;

	boolean DEFAULT_PRODUCER_IGNORE_ERROR = true;
	long DEFAULT_PRODUCER_ACK_TIMEOUT = 0L;
	String DEFAULT_PRODUCER_PARTITION_KEY = "database";
	String DEFAULT_PRODUCER_TYPE = "stdout";

    String DEFAULT_METRICS_PREFIX = "MaxwellMetrics";
	boolean DEFAULT_METRCS_JVM = false;

	String GTID_MODE_ENV = "GTID_MODE";

	void validate();

	MaxwellMysqlConfig getReplicationMysql();

	MaxwellMysqlConfig getSchemaMysql();

	MaxwellMysqlConfig getMaxwellMysql();

	MaxwellFilter getFilter();

	Boolean getGtidMode();

	String getDatabaseName();

	String getBootstrapperType();

	String getProducerType();

	String getProducerFactory();

	Properties getCustomProducerProperties();

	boolean isIgnoreProducerError();

	String getProducerPartitionKey();

	void setProducerPartitionKey(String producerPartitionKey);

	String getProducerPartitionColumns();

	void setProducerPartitionColumns(String producerPartitionColumns);

	String getProducerPartitionFallback();

	void setProducerPartitionFallback(String producerPartitionFallback);

	Long getProducerAckTimeout();

	MaxwellOutputConfig getOutputConfig();

	String getLogLevel();

	String getMetricsPrefix();

	String getMetricsReportingType();

	boolean isMetricsJvm();

	String getClientID();

	Long getReplicaServerID();

	Position getInitPosition();

	boolean isReplayMode();

	boolean isMasterRecovery();

}
