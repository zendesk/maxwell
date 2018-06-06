package com.zendesk.maxwell.api.config;

import com.zendesk.maxwell.api.config.*;
import com.zendesk.maxwell.api.replication.Position;

import java.util.Properties;

public interface MaxwellConfig {
	String GTID_MODE_ENV = "GTID_MODE";

	void validate();

	MaxwellMysqlConfig getReplicationMysql();

	MaxwellMysqlConfig getSchemaMysql();

	MaxwellMysqlConfig getMaxwellMysql();

	MaxwellFilter getFilter();

	Boolean getGtidMode();

	String getDatabaseName();

	String getCustomProducerFactory();

	Properties getCustomProducerProperties();

	String getProducerType();

	String getBootstrapperType();

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

	boolean isIgnoreProducerError();
}
