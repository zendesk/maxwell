package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.bootstrap.NoOpBootstrapper;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ReadOnlyMysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.PositionStoreThread;

import com.zendesk.maxwell.util.StoppableTask;
import com.zendesk.maxwell.util.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

public class MaxwellContext {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

	private final ConnectionPool replicationConnectionPool;
	private final ConnectionPool maxwellConnectionPool;
	private final ConnectionPool rawMaxwellConnectionPool;
	private final ConnectionPool schemaConnectionPool;
	private final MaxwellConfig config;
	private MysqlPositionStore positionStore;
	private PositionStoreThread positionStoreThread;
	private Long serverID;
	private BinlogPosition initialPosition;
	private CaseSensitivity caseSensitivity;
	private AbstractProducer producer;
	private TaskManager taskManager;
	private volatile Exception error;

	private Integer mysqlMajorVersion;
	private Integer mysqlMinorVersion;

	public MaxwellContext(MaxwellConfig config) throws SQLException {
		this.config = config;
		this.taskManager = new TaskManager();

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

		if ( this.getConfig().replayMode ) {
			this.positionStore = new ReadOnlyMysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID, config.gtidMode);
		} else {
			this.positionStore = new MysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID, config.gtidMode);
		}
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

	public void start() {
		getPositionStoreThread(); // boot up thread explicitly.
	}

	public void heartbeat() throws Exception {
		this.positionStore.heartbeat();
	}

	public void addTask(StoppableTask task) {
		this.taskManager.add(task);
	}

	public void terminate() {
		terminate(null);
	}

	public void terminate(Exception error) {
		if (this.error == null) {
			this.error = error;
		}
		if (taskManager.stop(error)) {
			this.replicationConnectionPool.release();
			this.maxwellConnectionPool.release();
			this.rawMaxwellConnectionPool.release();
		}
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


	public BinlogPosition getInitialPosition() throws SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = this.positionStore.get();
		return this.initialPosition;
	}

	public RecoveryInfo getRecoveryInfo() throws SQLException {
		return this.positionStore.getRecoveryInfo(config);
	}

	public void setPosition(RowMap r) throws SQLException {
		if ( r.isTXCommit() )
			this.setPosition(r.getPosition());
	}

	public void setPosition(BinlogPosition position) {
		this.getPositionStoreThread().setPosition(position);
	}

	public BinlogPosition getPosition() throws SQLException {
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


	private void fetchMysqlVersion() throws SQLException {
		if ( mysqlMajorVersion == null ) {
			try ( Connection c = getReplicationConnection() ) {
				DatabaseMetaData meta = c.getMetaData();
				mysqlMajorVersion = meta.getDatabaseMajorVersion();
				mysqlMinorVersion = meta.getDatabaseMinorVersion();
			}
		}
	}

	public boolean shouldHeartbeat() throws SQLException {
		fetchMysqlVersion();
		return mysqlMajorVersion >= 5 && mysqlMinorVersion >= 5;
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
		case "profiler":
			this.producer = new ProfilerProducer(this);
			break;
		case "stdout":
			this.producer = new StdoutProducer(this);
			break;
		case "buffer":
			this.producer = new BufferedProducer(this, this.config.bufferedProducerSize);
			break;
		case "none":
			this.producer = null;
			break;
		default:
			throw new RuntimeException("Unknown producer type: " + this.config.producerType);
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

	public MaxwellFilter getFilter() {
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

	public void probeConnections() throws SQLException {
		probePool(this.rawMaxwellConnectionPool, this.config.maxwellMysql.getConnectionURI(false));

		if ( this.maxwellConnectionPool != this.replicationConnectionPool )
			probePool(this.replicationConnectionPool, this.config.replicationMysql.getConnectionURI());
	}

}
