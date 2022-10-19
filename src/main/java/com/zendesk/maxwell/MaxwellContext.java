package com.zendesk.maxwell;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zendesk.maxwell.bootstrap.BootstrapController;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.monitoring.*;
import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.*;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlSchemaCompactor;
import com.zendesk.maxwell.schema.PositionStoreThread;
import com.zendesk.maxwell.schema.ReadOnlyMysqlPositionStore;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.RunLoopProcess;
import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zendesk.maxwell.util.ConnectionPool;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class containing runtime state
 */
public class MaxwellContext {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

	private ConnectionPool replicationConnectionPool;
	private ConnectionPool maxwellConnectionPool;
	private ConnectionPool rawMaxwellConnectionPool;
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

	private BootstrapController bootstrapController;
	private Thread bootstrapControllerThread;

	private Boolean isMariaDB;

	/**
	 * Contains various Maxwell metrics
	 */
	public MetricRegistry metricRegistry;

	/**
	 * Contains Maxwell health checks
	 */
	public HealthCheckRegistry healthCheckRegistry;

	/**
	 * Create a runtime context from a configuration object
	 * @param config Maxwell configuration
	 * @throws SQLException if there's issues connecting to the database
	 * @throws URISyntaxException if there's issues building database URIs
	 */
	public MaxwellContext(MaxwellConfig config) throws SQLException, URISyntaxException {
		this.config = config;
		this.config.validate();
		this.taskManager = new TaskManager();

		this.metricRegistry = config.metricRegistry;
		if ( this.metricRegistry == null )
			this.metricRegistry = new MetricRegistry();

		this.metrics = new MaxwellMetrics(config, this.metricRegistry);

		this.replicationConnectionPool = new C3P0ConnectionPool(
			config.replicationMysql.getConnectionURI(false),
			config.replicationMysql.user,
			config.replicationMysql.password
		);

		this.replicationConnectionPool.probe();

		if (config.schemaMysql.host == null) {
			this.schemaConnectionPool = null;
		} else {
			this.schemaConnectionPool = new C3P0ConnectionPool(
				config.schemaMysql.getConnectionURI(false),
				config.schemaMysql.user,
				config.schemaMysql.password
			);
			this.schemaConnectionPool.probe();
		}

		this.rawMaxwellConnectionPool = new C3P0ConnectionPool(
			config.maxwellMysql.getConnectionURI(false),
			config.maxwellMysql.user,
			config.maxwellMysql.password
		);
		this.rawMaxwellConnectionPool.probe();

		this.maxwellConnectionPool = new C3P0ConnectionPool(
			config.maxwellMysql.getConnectionURI(),
			config.maxwellMysql.user,
			config.maxwellMysql.password
		);
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

		this.healthCheckRegistry = config.healthCheckRegistry;
		if ( this.healthCheckRegistry == null )
			this.healthCheckRegistry = new HealthCheckRegistry();
	}

	/**
	 * Get Maxwell configuration used in this context
	 * @return the Maxwell configuration
	 */
	public MaxwellConfig getConfig() {
		return this.config;
	}

	/**
	 * Get the a connection from the replication pool
	 * @return a connection to the replication pool
	 * @throws SQLException if we can't connect
	 */
	public Connection getReplicationConnection() throws SQLException {
		return this.replicationConnectionPool.getConnection();
	}

	/**
	 * Get the replication pool
	 * @return the replication (connection to replicate from) connection pool
	 */
	public ConnectionPool getReplicationConnectionPool() { return replicationConnectionPool; }

	/**
	 * Get a connection from the maxwell (metadata) pool
	 * @return the maxwell (connection to store metadata in) connection pool
	 */
	public ConnectionPool getMaxwellConnectionPool() { return maxwellConnectionPool; }

	/**
	 * Get a connection from the schema pool
	 * @return the schema (connection to capture from) connection pool
	 */
	public ConnectionPool getSchemaConnectionPool() {
		if (this.schemaConnectionPool != null) {
			return schemaConnectionPool;
		}

		return replicationConnectionPool;
	}

	/**
	 * get a connection from the maxwell pool
	 * @return a connection from the maxwell pool
	 * @throws SQLException if we can't connect
	 */
	public Connection getMaxwellConnection() throws SQLException {
		return this.maxwellConnectionPool.getConnection();
	}

	/**
	 * get a database-less connection from the maxwell pool
	 *
	 * Used to create the maxwell schema.
	 * @return a connection to the maxwell pool, without a database name specific
	 * @throws SQLException if we can't connect
	 */
	public Connection getRawMaxwellConnection() throws SQLException {
		return rawMaxwellConnectionPool.getConnection();
	}

	/**
	 * Start the HTTP server and position store thread
	 * @throws IOException if the HTTP server can't be started
	 */
	public void start() throws IOException {
		MaxwellHTTPServer.startIfRequired(this);
		getPositionStoreThread();
	}

	/**
	 * Manually trigger a heartbeat to be sent
	 * @return Timestamp of the heartbeeat
	 * @throws Exception If we can't send a heartbeat
	 */
	public long heartbeat() throws Exception {
		return this.positionStore.heartbeat();
	}

	/**
	 * Add a task (usually a thread) that will be stopped upon shutdown
	 * @param task The task
	 */
	public void addTask(StoppableTask task) {
		this.taskManager.add(task);
	}

	/**
	 * Begin the maxwell shutdown process.
	 * <ul>
	 *    <li>Shuts down the {@link #replicator}</li>
	 *    <li>Calls {@link TaskManager#stop}</li>
	 *    <li>Stops metrics collection</li>
	 *    <li>Destroys all database pools</li>
	 * </ul>
	 * @return A thread that will complete shutdown.
	 */
	public Thread terminate() {
		return terminate(null);
	}

	/**
	 * Begin the Maxwell shutdown process
	 * @param error An exception that caused the shutdown, or null
	 * @return A thread that will complete shutdown.
	 * @see #terminate()
	 */
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
			this.metrics.stop();

			this.replicationConnectionPool.release();
			this.replicationConnectionPool = null;

			this.maxwellConnectionPool.release();
			this.maxwellConnectionPool = null;

			this.rawMaxwellConnectionPool.release();
			this.rawMaxwellConnectionPool = null;

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

				final boolean isShutdownComplete = shutdownComplete.get();
				LOGGER.debug("Shutdown complete: {}", isShutdownComplete);
				if (!isShutdownComplete) {
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

	private Thread startTask(RunLoopProcess task, String name) {
		Thread t = new Thread(() -> {
			try {
				task.runLoop();
			} catch (Exception e) {
				LOGGER.error("exception in thread: " + name, e);
				try {
					this.terminate(e);
				} catch (Exception exception) {
					exception.printStackTrace();
				}
			}
		}, name);

		t.start();
		addTask(task);
		return t;
	}

	/**
	 * Get the Exception that triggered shutdown
	 * @return An error that caused maxwell to shutdown
	 */
	public Exception getError() {
		return error;
	}

	/**
	 * Get or spawn a thread that persists the current position into the metadata database.
	 * @return Position store thread
	 */
	public PositionStoreThread getPositionStoreThread() {
		if ( this.positionStoreThread == null ) {
			this.positionStoreThread = new PositionStoreThread(this.positionStore, this);
			this.positionStoreThread.start();
			addTask(positionStoreThread);
		}
		return this.positionStoreThread;
	}


	/**
	 * Retrieve Maxwell's starting position from the metadata database
	 * @return The initial binlog position
	 * @throws SQLException If the position can't be retrieved from the database
	 */
	public Position getInitialPosition() throws SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = this.positionStore.get();
		return this.initialPosition;
	}

	/**
	 * Finds the most recent position any client has reached on the server
	 * @return A binlog position or NULL
	 * @throws SQLException If an error is encountered fetching the position
	 * @see MysqlPositionStore#getLatestFromAnyClient()
	 */
	public Position getOtherClientPosition() throws SQLException {
		return this.positionStore.getLatestFromAnyClient();
	}

	/**
	 * Build a {@link RecoveryInfo} object, used in non-GTID master failover
	 * @return Information used to recover a master position, or NULL
	 * @throws SQLException If we have database issues
	 * @see MysqlPositionStore#getRecoveryInfo(MaxwellConfig)
	 */
	public RecoveryInfo getRecoveryInfo() throws SQLException {
		return this.positionStore.getRecoveryInfo(config);
	}

	/**
	 * If the passed {@link RowMap} is a transaction-commit, update maxwell's position
	 * @param r A processed Rowmap
	 */
	public void setPosition(RowMap r) {
		if ( r.isTXCommit() )
			this.setPosition(r.getNextPosition());
	}

	/**
	 * Set Maxwell's next binlog position
	 * @param position The new position
	 */
	public void setPosition(Position position) {
		if ( position == null )
			return;

		this.getPositionStoreThread().setPosition(position);
	}

	/**
	 * Get the last stored binlog position
	 * @return The last binlog position set
	 * @throws SQLException If we have database issues
	 */
	public Position getPosition() throws SQLException {
		return this.getPositionStoreThread().getPosition();
	}

	/**
	 * Get the position store service object
	 * @return The mysql position store
	 */
	public MysqlPositionStore getPositionStore() {
		return this.positionStore;
	}

	/**
	 * Get the replication connection's server id
	 * @return a server id
	 * @throws SQLException if we have connection issues
	 */
	public Long getServerID() throws SQLException {
		if ( this.serverID != null)
			return this.serverID;

		try ( Connection c = getReplicationConnection();
		      Statement s = c.createStatement();
		      ResultSet rs = s.executeQuery("SELECT @@server_id as server_id") ) {
			if ( !rs.next() ) {
				throw new RuntimeException("Could not retrieve server_id!");
			}
			this.serverID = rs.getLong("server_id");
			return this.serverID;
		}
	}

	/**
	 * Get the replication connection's mysql version
	 * @return The mysql version
	 * @throws SQLException if we have connection issues
	 */
	public MysqlVersion getMysqlVersion() throws SQLException {
		if ( mysqlVersion == null ) {
			try ( Connection c = getReplicationConnection() ) {
				mysqlVersion = MysqlVersion.capture(c);
			}
		}
		return mysqlVersion;
	}

	/**
	 * Get the replication connection's case sensitivity settings
	 * @return case sensitivity settings
	 * @throws SQLException if we have connection issues
	 */
	public CaseSensitivity getCaseSensitivity() throws SQLException {
		if ( this.caseSensitivity == null ) {
			try (Connection c = getReplicationConnection()) {
				this.caseSensitivity = MaxwellMysqlStatus.captureCaseSensitivity(c);
			}
		}
		return this.caseSensitivity;
	}

	/**
	 * get or build an {@link AbstractProducer} based on settings in {@link #config}
	 * @return A producer
	 * @throws IOException if there's trouble instantiating the producer
	 */
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
				this.producer = new MaxwellSQSProducer(this, this.config.sqsQueueUri, this.config.sqsServiceEndpoint, this.config.sqsSigningRegion);
				break;
			case "sns":
				this.producer = new MaxwellSNSProducer(this, this.config.snsTopic);
				break;
			case "nats":
				this.producer = new NatsProducer(this);
				break;
			case "pubsub":
				this.producer = new MaxwellPubsubProducer(this, this.config.pubsubProjectId, this.config.pubsubTopic, this.config.ddlPubsubTopic, this.config.pubsubMessageOrderingKey, this.config.pubsubEmulator);
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
				this.producer = new MaxwellRedisProducer(this);
				break;
			case "bigquery":
				this.producer = new MaxwellBigQueryProducer(this, this.config.bigQueryProjectId, this.config.bigQueryDataset, this.config.bigQueryTable);
				break;
			case "none":
				this.producer = new NoneProducer(this);
				break;
			case "custom":
				// if we're here we missed specifying producer factory
				throw new RuntimeException("Please specify --custom_producer.factory!");
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

	/**
	 * only used in test code.  interrupt the bootstrap thread to quicken tests.
	 */
	public void runBootstrapNow() {
		if ( this.bootstrapControllerThread != null ) {
			this.bootstrapControllerThread.interrupt();
		}
	}

	/**
	 * get or start a {@link BootstrapController}
	 * @param currentSchemaID the currently active mysql schema
	 * @return a bootstrap controller
	 * @throws IOException if the bootstrap thread can't be started
	 */
	public synchronized BootstrapController getBootstrapController(Long currentSchemaID) throws IOException {
		if ( this.bootstrapController != null ) {
			return this.bootstrapController;
		}

		if ( this.config.bootstrapperType.equals("none") )
			return null;

		SynchronousBootstrapper bootstrapper = new SynchronousBootstrapper(this);
		this.bootstrapController = new BootstrapController(
			this.getMaxwellConnectionPool(),
			this.getProducer(),
			bootstrapper,
			this.config.clientID,
			this.config.bootstrapperType.equals("sync"),
			currentSchemaID
		);

		this.bootstrapControllerThread = this.startTask(this.bootstrapController, "maxwell-bootstrap-controller");

		return this.bootstrapController;
	}

	/**
	 * get or start a {@link MysqlSchemaCompactor}
	 * @throws SQLException if we have connection issues
	 */
	public void startSchemaCompactor() throws SQLException {
		if ( this.config.maxSchemaDeltas == null )
			return;

		MysqlSchemaCompactor compactor = new MysqlSchemaCompactor(
				this.config.maxSchemaDeltas,
				this.getMaxwellConnectionPool(),
				this.config.clientID,
				this.getServerID(),
				this.getCaseSensitivity()
		);

		this.startTask(compactor, "maxwell-schema-compactor");
	}

	/**
	 * get the current active filter
	 * @return the currently active Filter
	 */
	public Filter getFilter() {
		return config.filter;
	}

	/**
	 * Get the replayMode flag
	 * @return whether we are in "replay mode" (--replay)
	 */
	public boolean getReplayMode() {
		return this.config.replayMode;
	}

	/**
	 * Set the current binlog replicator
	 * @param replicator the replicator
	 */
	public void setReplicator(Replicator replicator) {
		this.addTask(replicator);
		this.replicator = replicator;
	}

	/**
	 * Get the current metrics registry
	 * @return the current metrics registry
	 */
	public Metrics getMetrics() {
		return metrics;
	}

	/**
	 * Get the heartbeat notifier object, which can be asked to broadcast heartbeats
	 * @return a heartbeat notifier
	 */
	public HeartbeatNotifier getHeartbeatNotifier() {
		return heartbeatNotifier;
	}

	/**
	 * Get the context for maxwell diagnostics
	 * @return the maxwell diagnostic context
	 */
	public MaxwellDiagnosticContext getDiagnosticContext() {
		return this.diagnosticContext;
	}

	/**
	 * Is the replication host running MariaDB?
	 * @return mariadbornot
	 */
	public boolean isMariaDB() {
		if ( this.isMariaDB == null ) {
			try ( Connection c = this.getReplicationConnection() ) {
				this.isMariaDB = MaxwellMysqlStatus.isMaria(c);
			} catch ( SQLException e ) {
				return false;
			}
		}

		return this.isMariaDB;
	}
}
