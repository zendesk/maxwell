package com.zendesk.maxwell.core.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.core.replication.Position;

import java.util.Properties;
import java.util.regex.Pattern;

public interface MaxwellConfig {
	String GTID_MODE_ENV = "GTID_MODE";

	static Pattern compileStringToPattern(String name) throws MaxwellInvalidFilterException {
		name = name.trim();
		if ( name.startsWith("/") ) {
			if ( !name.endsWith("/") ) {
				throw new MaxwellInvalidFilterException("Invalid regular expression: " + name);
			}
			return Pattern.compile(name.substring(1, name.length() - 1));
		} else {
			return Pattern.compile("^" + Pattern.quote(name) + "$");
		}
	}

	void validate();

	BaseMaxwellMysqlConfig getReplicationMysql();

	BaseMaxwellMysqlConfig getSchemaMysql();

	BaseMaxwellMysqlConfig getMaxwellMysql();

	MaxwellFilter getFilter();

	Boolean getGtidMode();

	String getDatabaseName();

	String getIncludeDatabases();

	String getExcludeDatabases();

	String getIncludeTables();

	String getExcludeTables();

	String getExcludeColumns();

	String getBlacklistDatabases();

	String getBlacklistTables();

	String getIncludeColumnValues();

	ProducerFactory getProducerFactory();

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

	BaseMaxwellOutputConfig getOutputConfig();

	String getLogLevel();

	MetricRegistry getMetricRegistry();

	HealthCheckRegistry getHealthCheckRegistry();

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

	BaseMaxwellDiagnosticConfig getDiagnosticConfig();

	String getClientID();

	Long getReplicaServerID();

	Position getInitPosition();

	boolean isReplayMode();

	boolean isMasterRecovery();

	boolean isIgnoreProducerError();
}
