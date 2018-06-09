package com.zendesk.maxwell.api.config;

import com.zendesk.maxwell.api.replication.Position;

import java.util.Properties;

public interface MaxwellConfig {
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

	int DEFAULT_HTTP_PORT = 8080;
	String DEFAULT_HTTP_PATH_PREFIX = "/";

    String DEFAULT_METRICS_PREFIX = "MaxwellMetrics";
    long DEFAULT_METRITCS_SLF4J_INTERVAL = 60L;
	String DEFAULT_METRICS_DATADOG_TYPE = "udp";
	String DEFAULT_METRICS_DATADOG_TAGS = "";
	String DEFAULT_METRICS_DATADOG_APIKEY = "";
	String DEFAULT_METRICS_DATADOG_HOST = "localhost";
	int DEFAULT_METRICS_DATADOG_PORT = 8125;
	long DEFAULT_METRICS_DATADOG_INTERVAL = 60L;
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

	int getHttpPort();

	String getHttpBindAddress();

	String getHttpPathPrefix();

	String getMetricsPrefix();

	String getMetricsReportingType();

	Long getMetricsSlf4jInterval();

	String getMetricsDatadogType();

	String getMetricsDatadogTags();

	String getMetricsDatadogAPIKey();

	String getMetricsDatadogHost();

	int getMetricsDatadogPort();

	Long getMetricsDatadogInterval();

	boolean isMetricsJvm();

	MaxwellDiagnosticConfig getDiagnosticConfig();

	String getClientID();

	Long getReplicaServerID();

	Position getInitPosition();

	boolean isReplayMode();

	boolean isMasterRecovery();

}
