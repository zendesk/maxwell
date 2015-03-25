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

public class Maxwell {
	private Schema schema;
	private MaxwellConfig config;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	private void initFirstRun(Connection connection) throws SQLException, IOException {
		LOGGER.info("Maxwell is capturing initial schema");
		SchemaCapturer capturer = new SchemaCapturer(connection);
		this.schema = capturer.capture();

		BinlogPosition pos = BinlogPosition.capture(connection);
		SchemaStore store = new SchemaStore(connection, this.schema, pos);
		store.save();

		this.config.setInitialPosition(pos);
	}

	private void ensureRowReplicationOn(Connection c) throws SQLException, MaxwellCompatibilityError {
		ResultSet rs;
		String status, checked;
		String usage = " Ensure mysql row-format binlog is setup.";
		rs = c.createStatement().executeQuery("SHOW VARIABLES LIKE 'log_bin'");
		if (!rs.next())
			throw new MaxwellCompatibilityError("Error checking log_bin state." + usage);
		status = rs.getString("Value");
		checked = rs.getString("Variable_name");
		LOGGER.info("Checking Binlog configuration: " + checked + " : " + status);
		if(!checked.equals("log_bin") || !status.equals("ON")) {
			throw new MaxwellCompatibilityError("log_bin status is not ON." + usage);
		}

		rs = c.createStatement().executeQuery("SHOW VARIABLES LIKE 'binlog_format'");
		if(!rs.next())
			throw new MaxwellCompatibilityError("Cannot check binlog_format" + usage);
		status = rs.getString("Value");
		checked = rs.getString("Variable_name");
		LOGGER.info("Checking Binlog configuration: " + checked + " : " + status);
		if(!checked.equals("binlog_format") || !status.equals("ROW")) {
			throw new MaxwellCompatibilityError("binlog_format is not ROW." + usage);
		}
	}

	private void run(String[] args) throws Exception {
		this.config = MaxwellConfig.buildConfig("config.properties", args);

		try ( Connection connection = this.config.getConnectionPool().getConnection() ) {
			ensureRowReplicationOn(connection);
			SchemaStore.ensureMaxwellSchema(connection);

			if ( this.config.getInitialPosition() != null ) {
				LOGGER.info("Maxwell is booting, starting at " + this.config.getInitialPosition());
				SchemaStore store = SchemaStore.restore(connection, this.config.getInitialPosition());
				this.schema = store.getSchema();
			} else {
				initFirstRun(connection);
			}
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
