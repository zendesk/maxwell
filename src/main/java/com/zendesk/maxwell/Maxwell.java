package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;

public class Maxwell {
	private Schema schema;
	private MaxwellConfig config;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	private void initFirstRun() throws SQLException, IOException {
		Connection connection = this.config.getMasterConnection();

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

		SchemaStore.ensureMaxwellSchema(this.config.getMasterConnection());

		if ( this.config.getInitialPosition() != null ) {
			LOGGER.info("Maxwell is booting, starting at " + this.config.getInitialPosition());
			SchemaStore store = SchemaStore.restore(this.config.getMasterConnection(), this.config.getInitialPosition());
			this.schema = store.getSchema();
		} else {
			initFirstRun();
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
