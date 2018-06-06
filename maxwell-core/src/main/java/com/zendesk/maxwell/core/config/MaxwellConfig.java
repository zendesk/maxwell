package com.zendesk.maxwell.core.config;

import com.zendesk.maxwell.core.MaxwellInvalidFilterException;
import com.zendesk.maxwell.core.producer.ProducerFactory;
import com.zendesk.maxwell.api.replication.Position;

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

	MaxwellMysqlConfig getReplicationMysql();

	MaxwellMysqlConfig getSchemaMysql();

	MaxwellMysqlConfig getMaxwellMysql();

	MaxwellFilter getFilter();

	Boolean getGtidMode();

	String getDatabaseName();

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
