package com.zendesk.maxwell;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zendesk.maxwell.producer.*;
import com.zendesk.maxwell.schema.SchemaPosition;

import snaq.db.ConnectionPool;

public class MaxwellContext {
	private final ConnectionPool connectionPool;
	private final MaxwellConfig config;
	private SchemaPosition schemaPosition;
	private Long serverID;
	private BinlogPosition initialPosition;

	public MaxwellContext(MaxwellConfig config) {
		this.config = config;
		this.connectionPool = new ConnectionPool("MaxwellConnectionPool", 10, 0, 10,
				config.getConnectionURI(), config.mysqlUser, config.mysqlPassword);
	}

	public MaxwellConfig getConfig() {
		return this.config;
	}

	public ConnectionPool getConnectionPool() {
		return this.connectionPool;
	}

	public void terminate() {
		if ( this.schemaPosition != null ) {
			this.schemaPosition.stop();
		}
		this.connectionPool.release();
	}

	private SchemaPosition getSchemaPosition() throws SQLException {
		if ( this.schemaPosition == null ) {
			this.schemaPosition = new SchemaPosition(this.getConnectionPool(), this.getServerID());
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

	public void setInitialPosition(BinlogPosition position) throws SQLException {
		this.getSchemaPosition().set(position);
	}

	public void setInitialPositionSync(BinlogPosition position) throws SQLException {
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
		case "stdout":
		default:
			return new StdoutProducer(this);
		}
	}

}
