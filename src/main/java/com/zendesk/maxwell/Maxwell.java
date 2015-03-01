package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;

public class Maxwell {
	private Schema schema;
	private MaxwellConfig config;

	private void initFirstRun() throws SQLException, IOException {
		Connection connection = this.config.getMasterConnection();

		SchemaStore.createMaxwellSchema(connection);

		SchemaCapturer capturer = new SchemaCapturer(connection);
		this.schema = capturer.capture();

		BinlogPosition pos = BinlogPosition.capture(connection);
		SchemaStore store = new SchemaStore(connection, this.schema, pos);
		store.save();

		this.config.setInitialPosition(pos);
	}

	private void run(String[] args) throws Exception {
		if ( args.length < 1 ) {
			System.err.println("Usage: bin/maxwell config.properties");
			System.exit(1);
		}

		this.config = MaxwellConfig.fromPropfile(args[0]);
		if ( this.config.getInitialPosition() != null ) {
			SchemaStore store = SchemaStore.restore(this.config.getMasterConnection(), this.config.getInitialPosition());
			this.schema = store.getSchema();
		} else {
			initFirstRun();
		}

		MaxwellParser p = new MaxwellParser(this.schema);
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
