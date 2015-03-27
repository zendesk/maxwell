package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class Maxwell {
	private Schema schema;
	private MaxwellConfig config;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	private void initFirstRun(Connection connection) throws SQLException, IOException, SchemaSyncError {
		LOGGER.info("Maxwell is capturing initial schema");
		SchemaCapturer capturer = new SchemaCapturer(connection);
		this.schema = capturer.capture();

		BinlogPosition pos = BinlogPosition.capture(connection);
		SchemaStore store = new SchemaStore(connection, this.schema, pos);
		store.save();

		this.config.setInitialPosition(pos);
	}

	private void run(String[] args) throws Exception {
		this.config = MaxwellConfig.buildConfig("config.properties", args);

		try ( Connection connection = this.config.getConnectionPool().getConnection() ) {
			MaxwellMysqlStatus.ensureMysqlState(connection);
			SchemaStore.ensureMaxwellSchema(connection);

			if ( this.config.getInitialPosition() != null ) {
				LOGGER.info("Maxwell is booting, starting at " + this.config.getInitialPosition());
				SchemaStore store = SchemaStore.restore(connection, this.config.getInitialPosition());
				this.schema = store.getSchema();
			} else {
				initFirstRun(connection);
			}
		} catch ( SQLException e ) {
			LOGGER.error("Failed to connect to mysql server @ " + this.config.getConnectionURI());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.config.getProducer();

		MaxwellParser p = new MaxwellParser(this.schema, producer);
		p.setConfig(this.config);
		p.run();

	}

	public static void main(String[] args) {
		try {
			new Maxwell().run(args);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
