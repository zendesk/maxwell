package com.zendesk.maxwell.core;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.config.MaxwellConfig;
import com.zendesk.maxwell.core.config.MaxwellFilter;
import com.zendesk.maxwell.core.monitoring.*;
import com.zendesk.maxwell.core.producer.Producer;
import com.zendesk.maxwell.core.producer.ProducerContext;
import com.zendesk.maxwell.core.recovery.RecoveryInfo;
import com.zendesk.maxwell.core.replication.*;
import com.zendesk.maxwell.core.row.RowMap;
import com.zendesk.maxwell.core.schema.MysqlPositionStore;
import com.zendesk.maxwell.core.schema.PositionStoreThread;
import com.zendesk.maxwell.core.schema.ReadOnlyMysqlPositionStore;
import com.zendesk.maxwell.core.util.StoppableTask;
import com.zendesk.maxwell.core.util.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class BaseMaxwellContext implements MaxwellContext {
	private static final Logger LOGGER = LoggerFactory.getLogger(BaseMaxwellContext.class);

	private final ConnectionPool replicationConnectionPool;
	private final ConnectionPool maxwellConnectionPool;
	private final ConnectionPool rawMaxwellConnectionPool;
	private final ConnectionPool schemaConnectionPool;
	private final MaxwellConfig config;
	private final MaxwellMetrics metrics;
	private final MysqlPositionStore positionStore;
	private PositionStoreThread positionStoreThread;
	private Long serverID;
	private Position initialPosition;
	private CaseSensitivity caseSensitivity;
	private ProducerContext producerContext;

	private final TaskManager taskManager;
	private final MetricRegistry metricRegistry;
	private final HealthCheckRegistry healthCheckRegistry;

	private volatile Exception error;

	private MysqlVersion mysqlVersion;
	private Replicator replicator;
	private Thread terminationThread;

	private final HeartbeatNotifier heartbeatNotifier;
	private final MaxwellDiagnosticContext diagnosticContext;

	private Consumer<MaxwellContext> onReplicationStartEventHandler;
	private Consumer<MaxwellContext> onReplicationCompletedEventHandler;
	private Consumer<MaxwellContext> onExecutionCompletedEventHandler;

	private final List<ContextStartListener> contextStartListenersEventHandler;

	public BaseMaxwellContext(MaxwellConfig config) throws SQLException, URISyntaxException {
		this(config, Collections.emptyList());
	}

	public BaseMaxwellContext(MaxwellConfig config, List<ContextStartListener> contextStartListenersEventHandler) throws SQLException, URISyntaxException {
		this.config = config;
		this.config.validate();

		this.taskManager = new TaskManager();
		this.metricRegistry = new MetricRegistry();
		this.healthCheckRegistry = new HealthCheckRegistry();

		this.metrics = new MaxwellMetrics(config, metricRegistry);
		this.contextStartListenersEventHandler = contextStartListenersEventHandler;

		this.replicationConnectionPool = new ConnectionPool("ReplicationConnectionPool", 10, 0, 10,
				config.getReplicationMysql().getConnectionURI(false), config.getReplicationMysql().getUser(), config.getReplicationMysql().getPassword());

		if (config.getSchemaMysql().getHost() == null) {
			this.schemaConnectionPool = null;
		} else {
			this.schemaConnectionPool = new ConnectionPool(
					"SchemaConnectionPool",
					10,
					0,
					10,
					config.getSchemaMysql().getConnectionURI(false),
					config.getSchemaMysql().getUser(),
					config.getSchemaMysql().getPassword());
		}

		this.rawMaxwellConnectionPool = new ConnectionPool("RawMaxwellConnectionPool", 1, 2, 100,
			config.getMaxwellMysql().getConnectionURI(false), config.getMaxwellMysql().getUser(), config.getMaxwellMysql().getPassword());

		this.maxwellConnectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
					config.getMaxwellMysql().getConnectionURI(), config.getMaxwellMysql().getUser(), config.getMaxwellMysql().getPassword());
		this.maxwellConnectionPool.setCaching(false);

		if ( this.config.getInitPosition() != null )
			this.initialPosition = this.config.getInitPosition();

		if (this.config.isReplayMode()) {
			this.positionStore = new ReadOnlyMysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.getClientID(), config.getGtidMode());
		} else {
			this.positionStore = new MysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.getClientID(), config.getGtidMode());
		}

		this.heartbeatNotifier = new HeartbeatNotifier();
		List<MaxwellDiagnostic> diagnostics = new ArrayList<>(Collections.singletonList(new BinlogConnectorDiagnostic(this)));
		this.diagnosticContext = new MaxwellDiagnosticContext(config.getDiagnosticConfig(), diagnostics);
	}

	@Override
	public MaxwellConfig getConfig() {
		return this.config;
	}

	@Override
	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	@Override
	public HealthCheckRegistry getHealthCheckRegistry() {
		return healthCheckRegistry;
	}

	@Override
	public Connection getReplicationConnection() throws SQLException {
		return this.replicationConnectionPool.getConnection();
	}

	@Override
	public ConnectionPool getReplicationConnectionPool() { return replicationConnectionPool; }
	@Override
	public ConnectionPool getMaxwellConnectionPool() { return maxwellConnectionPool; }

	@Override
	public ConnectionPool getSchemaConnectionPool() {
		if (this.schemaConnectionPool != null) {
			return schemaConnectionPool;
		}

		return replicationConnectionPool;
	}

	@Override
	public Connection getMaxwellConnection() throws SQLException {
		return this.maxwellConnectionPool.getConnection();
	}

	@Override
	public Connection getRawMaxwellConnection() throws SQLException {
		return rawMaxwellConnectionPool.getConnection();
	}

	@Override
	public void start() {
		contextStartListenersEventHandler.forEach(h -> h.onContextStart(this));
		getPositionStoreThread(); // boot up thread explicitly.
	}

	@Override
	public long heartbeat() throws Exception {
		return this.positionStore.heartbeat();
	}

	@Override
	public void addTask(StoppableTask task) {
		this.taskManager.add(task);
	}

	@Override
	public Thread terminate() {
		return terminate(null);
	}

	private void sendFinalHeartbeat() {
		long heartbeat = System.currentTimeMillis();
		LOGGER.info("Sending final heartbeat: " + heartbeat);
		try {
			this.replicator.stopAtHeartbeat(heartbeat);
			this.positionStore.heartbeat(heartbeat);
			long deadline = heartbeat + 5000L;
			while (positionStoreThread.getPosition().getLastHeartbeatRead() < heartbeat) {
				if (System.currentTimeMillis() > deadline) {
					LOGGER.warn("Timed out waiting for heartbeat " + heartbeat);
					break;
				}
				Thread.sleep(100);
			}
		} catch (Exception e) {
			LOGGER.error("Failed to send final heartbeat", e);
		}
	}

	private void shutdown(AtomicBoolean complete) {
		try {
			taskManager.stop(this.error);
			this.replicationConnectionPool.release();
			this.maxwellConnectionPool.release();
			this.rawMaxwellConnectionPool.release();
			complete.set(true);
		} catch (Exception e) {
			LOGGER.error("Exception occurred during shutdown:", e);
		}
	}

	private Thread spawnTerminateThread() {
		// Because terminate() may be called from a task thread
		// which won't end until we let its event loop progress,
		// we need to perform termination in a new thread
		final AtomicBoolean shutdownComplete = new AtomicBoolean(false);
		final BaseMaxwellContext self = this;
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				// Spawn an inner thread to perform shutdown
				final Thread shutdownThread = new Thread(new Runnable() {
					@Override
					public void run() {
						self.shutdown(shutdownComplete);
					}
				}, "shutdownThread");
				shutdownThread.start();

				// wait for its completion, timing out after 10s
				try {
					shutdownThread.join(10000L);
				} catch (InterruptedException e) {
					// ignore
				}

				LOGGER.debug("Shutdown complete: " + shutdownComplete.get());
				if (!shutdownComplete.get()) {
					LOGGER.error("Shutdown stalled - forcefully killing maxwell process");
					if (self.error != null) {
						LOGGER.error("Termination reason:", self.error);
					}
					Runtime.getRuntime().halt(1);
				}
			}
		}, "shutdownMonitor");
		thread.setDaemon(false);
		thread.start();
		return thread;
	}

	@Override
	public Thread terminate(Exception error) {
		if (this.error == null) {
			this.error = error;
		}

		if (taskManager.requestStop()) {
			if (this.error == null && this.replicator != null) {
				sendFinalHeartbeat();
			}
			this.terminationThread = spawnTerminateThread();
		}
		return this.terminationThread;
	}

	@Override
	public Exception getError() {
		return error;
	}

	@Override
	public PositionStoreThread getPositionStoreThread() {
		if ( this.positionStoreThread == null ) {
			this.positionStoreThread = new PositionStoreThread(this.positionStore, this);
			this.positionStoreThread.start();
			addTask(positionStoreThread);
		}
		return this.positionStoreThread;
	}


	@Override
	public Position getInitialPosition() throws SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = this.positionStore.get();
		return this.initialPosition;
	}

	@Override
	public Position getOtherClientPosition() throws SQLException {
		return this.positionStore.getLatestFromAnyClient();
	}

	@Override
	public RecoveryInfo getRecoveryInfo() throws SQLException {
		return this.positionStore.getRecoveryInfo(config);
	}

	@Override
	public void setPosition(RowMap r) {
		if ( r.isTXCommit() )
			this.setPosition(r.getPosition());
	}

	@Override
	public void setPosition(Position position) {
		this.getPositionStoreThread().setPosition(position);
	}

	@Override
	public Position getPosition() throws SQLException {
		return this.getPositionStoreThread().getPosition();
	}

	@Override
	public MysqlPositionStore getPositionStore() {
		return this.positionStore;
	}

	@Override
	public Long getServerID() throws SQLException {
		if ( this.serverID != null)
			return this.serverID;

		try ( Connection c = getReplicationConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SELECT @@server_id as server_id");
			if ( !rs.next() ) {
				throw new RuntimeException("Could not retrieve server_id!");
			}
			this.serverID = rs.getLong("server_id");
			return this.serverID;
		}
	}

	@Override
	public MysqlVersion getMysqlVersion() throws SQLException {
		if ( mysqlVersion == null ) {
			try ( Connection c = getReplicationConnection() ) {
				mysqlVersion = MysqlVersion.capture(c);
			}
		}
		return mysqlVersion;
	}

	@Override
	public boolean shouldHeartbeat() throws SQLException {
		return getMysqlVersion().atLeast(5,5);
	}

	@Override
	public CaseSensitivity getCaseSensitivity() throws SQLException {
		if ( this.caseSensitivity != null )
			return this.caseSensitivity;

		try ( Connection c = getReplicationConnection()) {
			ResultSet rs = c.createStatement().executeQuery("select @@lower_case_table_names");
			if ( !rs.next() )
				throw new RuntimeException("Could not retrieve @@lower_case_table_names!");

			int value = rs.getInt(1);
			switch(value) {
				case 0:
					this.caseSensitivity = CaseSensitivity.CASE_SENSITIVE;
					break;
				case 1:
					this.caseSensitivity = CaseSensitivity.CONVERT_TO_LOWER;
					break;
				case 2:
					this.caseSensitivity = CaseSensitivity.CONVERT_ON_COMPARE;
					break;
				default:
					throw new RuntimeException("Unknown value for @@lower_case_table_names: " + value);
			}
			return this.caseSensitivity;
		}
	}

	@Override
	public MaxwellFilter getFilter() {
		return config.getFilter();
	}

	@Override
	public boolean getReplayMode() {
		return this.config.isReplayMode();
	}

	private void probePool( ConnectionPool pool, String uri ) throws SQLException {
		try (Connection c = pool.getConnection()) {
			c.createStatement().execute("SELECT 1");
		} catch (SQLException e) {
			LOGGER.error("Could not connect to " + uri + ": " + e.getLocalizedMessage());
			throw (e);
		}
	}

	@Override
	public void probeConnections() throws SQLException, URISyntaxException {
		probePool(this.rawMaxwellConnectionPool, this.config.getMaxwellMysql().getConnectionURI(false));

		if ( this.maxwellConnectionPool != this.replicationConnectionPool )
			probePool(this.replicationConnectionPool, this.config.getReplicationMysql().getConnectionURI());
	}

	@Override
	public void setReplicator(Replicator replicator) {
		this.addTask(replicator);
		this.replicator = replicator;
	}

	@Override
	public Metrics getMetrics() {
		return metrics;
	}

	@Override
	public HeartbeatNotifier getHeartbeatNotifier() {
		return heartbeatNotifier;
	}

	@Override
	public MaxwellDiagnosticContext getDiagnosticContext() {
		return this.diagnosticContext;
	}

	@Override
	public Producer getProducer(){
		return getProducerContext().getProducer();
	}

	@Override
	public ProducerContext getProducerContext(){
		return getOptionalProducerContext().orElseThrow(() -> new IllegalStateException("No producer context initialized"));
	}

	@Override
	public Optional<ProducerContext> getOptionalProducerContext() {
		return Optional.ofNullable(producerContext);
	}

	@Override
	public void setProducerContext(ProducerContext producerContext) {
		this.producerContext = producerContext;
	}

	@Override
	public void configureOnReplicationStartEventHandler(Consumer<MaxwellContext> onReplicationStartEventHandler){
		this.onReplicationStartEventHandler = onReplicationStartEventHandler;
	}

	@Override
	public Optional<Consumer<MaxwellContext>> getOnReplicationStartEventHandler(){
		return Optional.ofNullable(onReplicationStartEventHandler);
	}

	@Override
	public void configureOnReplicationCompletedEventHandler(Consumer<MaxwellContext> onReplicationCompletedEventHandler){
		this.onReplicationCompletedEventHandler = onReplicationCompletedEventHandler;
	}

	@Override
	public Optional<Consumer<MaxwellContext>> getOnReplicationCompletedEventHandler(){
		return Optional.ofNullable(onReplicationCompletedEventHandler);
	}

	@Override
	public void configureOnExecutionCompletedEventHandler(Consumer<MaxwellContext> onExecutionCompletedEventHandler) {
		this.onExecutionCompletedEventHandler = onExecutionCompletedEventHandler;
	}

	@Override
	public Optional<Consumer<MaxwellContext>> getOnExecutionCompletedEventHandler(){
		return Optional.ofNullable(onExecutionCompletedEventHandler);
	}
}
