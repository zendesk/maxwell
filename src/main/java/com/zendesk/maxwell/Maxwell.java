package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;

public class Maxwell {
	private Connection connection;
	private BinlogPosition initialPosition;
	private Schema schema;

	private void initFirstRun() throws SQLException, IOException {
		this.initialPosition = BinlogPosition.capture(connection);

		SchemaCapturer capturer = new SchemaCapturer(connection);
		this.schema = capturer.capture();

		SchemaStore store = new SchemaStore(this.connection, this.schema, this.initialPosition);
		store.save();
	}

	private void run(String[] args) throws Exception {
		this.connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + 3306 + "/mysql", "root", "");
		if ( true ) {
			initFirstRun();
		}

		MaxwellParser p = new MaxwellParser(this.schema);
		p.setBinlogPosition(this.initialPosition);
		p.run();

	}

	public static void main(String[] args) {
		try {
			new Maxwell().run(args);
		} catch ( Exception e ) {
			System.out.println("Got exception!");
			System.out.println(e);
		}
	}
}
