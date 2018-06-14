package com.zendesk.maxwell;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.bootstrap.NoOpBootstrapper;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.monitoring.*;
import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.*;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.PositionStoreThread;
import com.zendesk.maxwell.schema.ReadOnlyMysqlPositionStore;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MaxwellContext {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

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
	private AbstractProducer producer;
	private final TaskManager taskManager;
	private volatile Exception error;

	private MysqlVersion mysqlVersion;
	private Replicator replicator;
	private Thread terminationThread;

	private final HeartbeatNotifier heartbeatNotifier;
	private final MaxwellDiagnosticContext diagnosticContext;

	public MaxwellContext(MaxwellConfig config) throws SQLException, URISyntaxException {
		this.config = config;
		this.config.validate();
		this.taskManager = new TaskManager();
		this.metrics = new MaxwellMetrics(config);

		this.replicationConnectionPool = new ConnectionPool("ReplicationConnectionPool", 10, 0, 10,
				config.replicationMysql.getConnectionURI(false), config.replicationMysql.user, config.replicationMysql.password);

		if (config.schemaMysql.host == null) {
			this.schemaConnectionPool = null;
		} else {
			this.schemaConnectionPool = new ConnectionPool(
					"SchemaConnectionPool",
					10,
					0,
					10,
					config.schemaMysql.getConnectionURI(false),
					config.schemaMysql.user,
					config.schemaMysql.password);
		}

		this.rawMaxwellConnectionPool = new ConnectionPool("RawMaxwellConnectionPool", 1, 2, 100,
			config.maxwellMysql.getConnectionURI(false), config.maxwellMysql.user, config.maxwellMysql.password);

		this.maxwellConnectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
					config.maxwellMysql.getConnectionURI(), config.maxwellMysql.user, config.maxwellMysql.password);
		this.maxwellConnectionPool.setCaching(false);

		if ( this.config.initPosition != null )
			this.initialPosition = this.config.initPosition;

		if ( this.config.replayMode ) {
			this.positionStore = new ReadOnlyMysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID, config.gtidMode);
		} else {
			this.positionStore = new MysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID, config.gtidMode);
		}

		this.heartbeatNotifier = new HeartbeatNotifier();
		List<MaxwellDiagnostic> diagnostics = new ArrayList<>(Collections.singletonList(new BinlogConnectorDiagnostic(this)));
		this.diagnosticContext = new MaxwellDiagnosticContext(config.diagnosticConfig, diagnostics);
	}

	public MaxwellConfig getConfig() {
		return this.config;
	}

	public Connection getReplicationConnection() throws SQLException {
		return this.replicationConnectionPool.getConnection();
	}

	public ConnectionPool getReplicationConnectionPool() { return replicationConnectionPool; }
	public ConnectionPool getMaxwellConnectionPool() { return maxwellConnectionPool; }

	public ConnectionPool getSchemaConnectionPool() {
		if (this.schemaConnectionPool != null) {
			return schemaConnectionPool;
		}

		return replicationConnectionPool;
	}

	public Connection getMaxwellConnection() throws SQLException {
		return this.maxwellConnectionPool.getConnection();
	}

	public Connection getRawMaxwellConnection() throws SQLException {
		return rawMaxwellConnectionPool.getConnection();
	}

	public void start() throws IOException {
		MaxwellHTTPServer.startIfRequired(this);
		getPositionStoreThread(); // boot up thread explicitly.
	}

	public long heartbeat() throws Exception {
		return this.positionStore.heartbeat();
	}

	public void addTask(StoppableTask task) {
		this.taskManager.add(task);
	}

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
		final MaxwellContext self = this;
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

	public Exception getError() {
		return error;
	}

	public PositionStoreThread getPositionStoreThread() {
		if ( this.positionStoreThread == null ) {
			this.positionStoreThread = new PositionStoreThread(this.positionStore, this);
			this.positionStoreThread.start();
			addTask(positionStoreThread);
		}
		return this.positionStoreThread;
	}


	public Position getInitialPosition() throws SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = this.positionStore.get();
		return this.initialPosition;
	}

	public Position getOtherClientPosition() throws SQLException {
		return this.positionStore.getLatestFromAnyClient();
	}

	public RecoveryInfo getRecoveryInfo() throws SQLException {
		return this.positionStore.getRecoveryInfo(config);
	}

	public void setPosition(RowMap r) {
		if ( r.isTXCommit() )
			this.setPosition(r.getNextPosition());
	}

	public void setPosition(Position position) {
		this.getPositionStoreThread().setPosition(position);
	}

	public Position getPosition() throws SQLException {
		return this.getPositionStoreThread().getPosition();
	}

	public MysqlPositionStore getPositionStore() {
		return this.positionStore;
	}

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

	public MysqlVersion getMysqlVersion() throws SQLException {
		if ( mysqlVersion == null ) {
			try ( Connection c = getReplicationConnection() ) {
				mysqlVersion = MysqlVersion.capture(c);
			}
		}
		return mysqlVersion;
	}

	public boolean shouldHeartbeat() throws SQLException {
		return getMysqlVersion().atLeast(5,5);
	}

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

	public AbstractProducer getProducer() throws IOException {
		if ( this.producer != null )
			return this.producer;

		if ( this.config.producerFactory != null ) {
			this.producer = this.config.producerFactory.createProducer(this);
		} else {
			switch ( this.config.producerType ) {
			case "file":
				this.producer = new FileProducer(this, this.config.outputFile);
				break;
			case "kafka":
				this.producer = new MaxwellKafkaProducer(this, this.config.getKafkaProperties(), this.config.kafkaTopic);
				break;
			case "kinesis":
				this.producer = new MaxwellKinesisProducer(this, this.config.kinesisStream);
				break;
			case "sqs":
				this.producer = new MaxwellSQSProducer(this, this.config.sqsQueueUri);
				break;
			case "pubsub":
				this.producer = new MaxwellPubsubProducer(this, this.config.pubsubProjectId, this.config.pubsubTopic, this.config.ddlPubsubTopic);
				break;
			case "profiler":
				this.producer = new ProfilerProducer(this);
				break;
			case "stdout":
				this.producer = new StdoutProducer(this);
				break;
			case "buffer":
				this.producer = new BufferedProducer(this, this.config.bufferedProducerSize);
				break;
			case "rabbitmq":
				this.producer = new RabbitmqProducer(this);
				break;
			case "redis":
				this.producer = new MaxwellRedisProducer(this, this.config.redisPubChannel, this.config.redisListKey, this.config.redisType);
				break;
			case "none":
				this.producer = null;
				break;
			default:
				throw new RuntimeException("Unknown producer type: " + this.config.producerType);
			}
		}

		if (this.producer != null && this.producer.getDiagnostic() != null) {
			diagnosticContext.diagnostics.add(producer.getDiagnostic());
		}

		StoppableTask task = null;
		if (producer != null) {
			task = producer.getStoppableTask();
		}
		if (task != null) {
			addTask(task);
		}
		return this.producer;
	}

	public AbstractBootstrapper getBootstrapper() throws IOException {
		switch ( this.config.bootstrapperType ) {
			case "async":
				return new AsynchronousBootstrapper(this);
			case "sync":
				return new SynchronousBootstrapper(this);
			default:
				return new NoOpBootstrapper(this);
		}

	}

	public Filter getFilter() {
		return config.filter;
	}

	public boolean getReplayMode() {
		return this.config.replayMode;
	}

	private void probePool( ConnectionPool pool, String uri ) throws SQLException {
		try (Connection c = pool.getConnection()) {
			c.createStatement().execute("SELECT 1");
		} catch (SQLException e) {
			LOGGER.error("Could not connect to " + uri + ": " + e.getLocalizedMessage());
			throw (e);
		}
	}

	public void probeConnections() throws SQLException, URISyntaxException {
		probePool(this.rawMaxwellConnectionPool, this.config.maxwellMysql.getConnectionURI(false));

		if ( this.maxwellConnectionPool != this.replicationConnectionPool )
			probePool(this.replicationConnectionPool, this.config.replicationMysql.getConnectionURI());
	}

	public void setReplicator(Replicator replicator) {
		this.addTask(replicator);
		this.replicator = replicator;
	}

	public Metrics getMetrics() {
		return metrics;
	}

	public HeartbeatNotifier getHeartbeatNotifier() {
		return heartbeatNotifier;
	}

	public MaxwellDiagnosticContext getDiagnosticContext() {
		return this.diagnosticContext;
	}
}
