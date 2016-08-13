package com.zendesk.maxwell;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.bootstrap.AsynchronousBootstrapper;
import com.zendesk.maxwell.bootstrap.NoOpBootstrapper;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.schema.ReadOnlyMysqlPositionStore;
import com.zendesk.maxwell.schema.MysqlPositionStore;
import com.zendesk.maxwell.schema.PositionStoreThread;

import com.zendesk.maxwell.schema.SchemaScavenger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

public class MaxwellContext {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

	private final ConnectionPool replicationConnectionPool;
	private final ConnectionPool maxwellConnectionPool;
	private final ConnectionPool rawMaxwellConnectionPool;
	private final MaxwellConfig config;
	private PositionStoreThread positionStoreThread;
	private Long serverID;
	private BinlogPosition initialPosition;
	private CaseSensitivity caseSensitivity;

	private Integer mysqlMajorVersion;
	private Integer mysqlMinorVersion;

	public MaxwellContext(MaxwellConfig config) {
		this.config = config;

		this.replicationConnectionPool = new ConnectionPool("ReplicationConnectionPool", 10, 0, 10,
				config.replicationMysql.getConnectionURI(), config.replicationMysql.user, config.replicationMysql.password);

		this.rawMaxwellConnectionPool = new ConnectionPool("RawMaxwellConenectionPool", 1, 2, 100,
			config.maxwellMysql.getConnectionURI(false), config.maxwellMysql.user, config.maxwellMysql.password);

		this.maxwellConnectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
					config.maxwellMysql.getConnectionURI(), config.maxwellMysql.user, config.maxwellMysql.password);
		this.maxwellConnectionPool.setCaching(false);

		if ( this.config.initPosition != null )
			this.initialPosition = this.config.initPosition;
	}

	public MaxwellConfig getConfig() {
		return this.config;
	}

	public Connection getReplicationConnection() throws SQLException {
		return this.replicationConnectionPool.getConnection();
	}

	public ConnectionPool getReplicationConnectionPool() { return replicationConnectionPool; }
	public ConnectionPool getMaxwellConnectionPool() { return maxwellConnectionPool; }

	public Connection getMaxwellConnection() throws SQLException {
		return this.maxwellConnectionPool.getConnection();
	}

	public Connection getRawMaxwellConnection() throws SQLException {
		return rawMaxwellConnectionPool.getConnection();
	}

	public void start() {
		SchemaScavenger s = new SchemaScavenger(this.maxwellConnectionPool);
		new Thread(s, "maxwell-schema-scavenger").start();
	}

	public void terminate() {
		if ( this.positionStoreThread != null ) {
			try {
				this.positionStoreThread.stopLoop();
				this.positionStoreThread = null;
			} catch (TimeoutException e) {
				LOGGER.error("got timeout trying to shutdown positionStore thread.");
			}
		}
		this.replicationConnectionPool.release();
		this.maxwellConnectionPool.release();
	}

	public PositionStoreThread getPositionStoreThread() throws SQLException {
		if ( this.positionStoreThread == null ) {
			MysqlPositionStore store;
			if ( this.getConfig().replayMode ) {
				store = new ReadOnlyMysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID);
			} else {
				store = new MysqlPositionStore(this.getMaxwellConnectionPool(), this.getServerID(), this.config.clientID);
			}
			this.positionStoreThread = new PositionStoreThread(store);
			this.positionStoreThread.start();
		}
		return this.positionStoreThread;
	}


	public BinlogPosition getInitialPosition() throws SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = getPositionStoreThread().getPosition();

		if ( this.initialPosition == null ) {
			try ( Connection connection = getReplicationConnection() ) {
				this.initialPosition = BinlogPosition.capture(connection);
				this.setPosition(this.initialPosition);
			}
		}

		return this.initialPosition;
	}

	public void setPosition(RowMap r) throws SQLException {
		if ( r.isTXCommit() )
			this.setPosition(r.getPosition());
	}

	public void setPosition(BinlogPosition position) throws SQLException {
		this.getPositionStoreThread().setPosition(position);
	}

	public BinlogPosition getPosition() throws SQLException {
		return this.getPositionStoreThread().getPosition();
	}

	public void ensurePositionThread() throws Exception {
		if ( this.positionStoreThread == null )
			return;

		Exception e = this.getPositionStoreThread().getException();
		if ( e != null ) {
			throw (e);
		}
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
		switch ( this.config.producerType ) {
		case "file":
			return new FileProducer(this, this.config.outputFile);
		case "kafka":
			return new MaxwellKafkaProducer(this, this.config.getKafkaProperties(), this.config.kafkaTopic);
		case "profiler":
			return new ProfilerProducer(this);
		case "stdout":
		default:
			return new StdoutProducer(this);
		}
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
		probePool(this.maxwellConnectionPool, this.config.maxwellMysql.getConnectionURI());

		if ( this.maxwellConnectionPool != this.replicationConnectionPool )
			probePool(this.replicationConnectionPool, this.config.replicationMysql.getConnectionURI());
	}
}
