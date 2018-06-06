package com.zendesk.maxwell.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.api.config.MaxwellConfig;
import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.core.monitoring.MaxwellDiagnosticContext;
import com.zendesk.maxwell.core.monitoring.Metrics;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerContext;
import com.zendesk.maxwell.core.recovery.RecoveryInfo;
import com.zendesk.maxwell.core.replication.HeartbeatNotifier;
import com.zendesk.maxwell.core.replication.MysqlVersion;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.schema.MysqlPositionStore;
import com.zendesk.maxwell.core.schema.PositionStoreThread;
import com.zendesk.maxwell.core.util.StoppableTask;
import snaq.db.ConnectionPool;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;

public interface MaxwellContext {
	MaxwellConfig getConfig();

	MetricRegistry getMetricRegistry();

	HealthCheckRegistry getHealthCheckRegistry();

	Connection getReplicationConnection() throws SQLException;

	ConnectionPool getReplicationConnectionPool();

	ConnectionPool getMaxwellConnectionPool();

	ConnectionPool getSchemaConnectionPool();

	Connection getMaxwellConnection() throws SQLException;

	Connection getRawMaxwellConnection() throws SQLException;

	void start();

	long heartbeat() throws Exception;

	void addTask(StoppableTask task);

	Thread terminate();

	Thread terminate(Exception error);

	Exception getError();

	PositionStoreThread getPositionStoreThread();

	Position getInitialPosition() throws SQLException;

	Position getOtherClientPosition() throws SQLException;

	RecoveryInfo getRecoveryInfo() throws SQLException;

	void setPosition(RowMap r);

	void setPosition(Position position);

	Position getPosition() throws SQLException;

	MysqlPositionStore getPositionStore();

	Long getServerID() throws SQLException;

	MysqlVersion getMysqlVersion() throws SQLException;

	boolean shouldHeartbeat() throws SQLException;

	CaseSensitivity getCaseSensitivity() throws SQLException;

	MaxwellFilter getFilter();

	boolean getReplayMode();

	void probeConnections() throws SQLException, URISyntaxException;

	void setReplicator(Replicator replicator);

	Metrics getMetrics();

	HeartbeatNotifier getHeartbeatNotifier();

	MaxwellDiagnosticContext getDiagnosticContext();

	Producer getProducer();

	ProducerContext getProducerContext();

	Optional<ProducerContext> getOptionalProducerContext();

	void setProducerContext(ProducerContext producerContext);

	void configureOnReplicationStartEventHandler(Consumer<MaxwellContext> onReplicationStartEventHandler);

	Optional<Consumer<MaxwellContext>> getOnReplicationStartEventHandler();

	void configureOnReplicationCompletedEventHandler(Consumer<MaxwellContext> onReplicationCompletedEventHandler);

	Optional<Consumer<MaxwellContext>> getOnReplicationCompletedEventHandler();

	void configureOnExecutionCompletedEventHandler(Consumer<MaxwellContext> onExecutionCompletedEventHandler);

	Optional<Consumer<MaxwellContext>> getOnExecutionCompletedEventHandler();
}
