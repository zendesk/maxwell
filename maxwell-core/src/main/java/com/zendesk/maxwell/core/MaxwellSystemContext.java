package com.zendesk.maxwell.core;

import com.zendesk.maxwell.api.config.MaxwellFilter;
import com.zendesk.maxwell.core.producer.ProducerContext;
import com.zendesk.maxwell.core.recovery.RecoveryInfo;
import com.zendesk.maxwell.core.replication.HeartbeatNotifier;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.replication.Replicator;
import com.zendesk.maxwell.core.schema.MysqlPositionStore;
import snaq.db.ConnectionPool;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Consumer;

public interface MaxwellSystemContext extends MaxwellContext {

	Connection getReplicationConnection() throws SQLException;

	ConnectionPool getReplicationConnectionPool();

	ConnectionPool getMaxwellConnectionPool();

	ConnectionPool getSchemaConnectionPool();

	Connection getMaxwellConnection() throws SQLException;

	Connection getRawMaxwellConnection() throws SQLException;

	void start();

	long heartbeat() throws Exception;

	Exception getError();

	Position getInitialPosition() throws SQLException;

	Position getOtherClientPosition() throws SQLException;

	RecoveryInfo getRecoveryInfo() throws SQLException;

	MysqlPositionStore getPositionStore();

	Long getServerID() throws SQLException;

	CaseSensitivity getCaseSensitivity() throws SQLException;

	MaxwellFilter getFilter();

	boolean getReplayMode();

	void probeConnections() throws SQLException, URISyntaxException;

	void setReplicator(Replicator replicator);

	HeartbeatNotifier getHeartbeatNotifier();

	void setProducerContext(ProducerContext producerContext);

	void configureOnReplicationStartEventHandler(Consumer<MaxwellContext> onReplicationStartEventHandler);

	Optional<Consumer<MaxwellContext>> getOnReplicationStartEventHandler();

	void configureOnReplicationCompletedEventHandler(Consumer<MaxwellContext> onReplicationCompletedEventHandler);

	Optional<Consumer<MaxwellContext>> getOnReplicationCompletedEventHandler();

	void configureOnExecutionCompletedEventHandler(Consumer<MaxwellContext> onExecutionCompletedEventHandler);

	Optional<Consumer<MaxwellContext>> getOnExecutionCompletedEventHandler();
}
