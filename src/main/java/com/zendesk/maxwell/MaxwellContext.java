package com.zendesk.maxwell;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.schema.ReadOnlySchemaPosition;
import com.zendesk.maxwell.schema.SchemaPosition;

import com.zendesk.maxwell.schema.SchemaScavenger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

public class MaxwellContext {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellContext.class);

	private final ConnectionPool connectionPool;
	private final MaxwellConfig config;
	private SchemaPosition schemaPosition;
	private Long serverID;
	private BinlogPosition initialPosition;

	public MaxwellContext(MaxwellConfig config) {
		this.config = config;
		this.connectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
				config.getConnectionURI(), config.mysqlUser, config.mysqlPassword);

		if ( this.config.initPosition != null )
			this.initialPosition = this.config.initPosition;
	}

	public MaxwellConfig getConfig() {
		return this.config;
	}

	public ConnectionPool getConnectionPool() {
		return this.connectionPool;
	}

	public void start() {
		SchemaScavenger s = new SchemaScavenger(this.connectionPool);
		new Thread(s).start();
	}

	public void terminate() {
		if ( this.schemaPosition != null ) {
			try {
				this.schemaPosition.stopLoop();
			} catch (TimeoutException e) {
				LOGGER.error("got timeout trying to shutdown schemaPosition thread.");
			}
		}
		this.connectionPool.release();
	}

	private SchemaPosition getSchemaPosition() throws SQLException {
		if ( this.schemaPosition == null ) {
			if ( this.getConfig().replayMode ) {
				this.schemaPosition = new ReadOnlySchemaPosition(this.getConnectionPool(), this.getServerID());
			} else {
				this.schemaPosition = new SchemaPosition(this.getConnectionPool(), this.getServerID());
			}

			this.schemaPosition.start();
		}
		return this.schemaPosition;
	}


	public BinlogPosition getInitialPosition() throws FileNotFoundException, IOException, SQLException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		this.initialPosition = getSchemaPosition().get();
		return this.initialPosition;
	}

	public void setPosition(RowMap r) throws SQLException {
		if ( r.isTXCommit() )
			this.setPosition(r.getPosition());
	}

	public void setPosition(BinlogPosition position) throws SQLException {
		this.getSchemaPosition().set(position);
	}

	public void setPositionSync(BinlogPosition position) throws SQLException {
		this.getSchemaPosition().setSync(position);
	}

	public void ensurePositionThread() throws SQLException {
		if ( this.schemaPosition == null )
			return;

		SQLException e = this.schemaPosition.getException();
		if ( e != null ) {
			throw (e);
		}
	}

	public Long getServerID() throws SQLException {
		if ( this.serverID != null)
			return this.serverID;

		try ( Connection c = getConnectionPool().getConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SELECT @@server_id as server_id");
			if ( !rs.next() ) {
				throw new RuntimeException("Could not retrieve server_id!");
			}
			this.serverID = rs.getLong("server_id");
			return this.serverID;
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

	public MaxwellFilter buildFilter() throws MaxwellInvalidFilterException {
		return new MaxwellFilter(config.includeDatabases,
			config.excludeDatabases,
			config.includeTables,
			config.excludeTables);
	}


	public boolean getReplayMode() {
		return this.config.replayMode;
	}

}
