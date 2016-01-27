package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.ddl.SchemaSyncError;

public class Maxwell {
	private Schema schema;
	private MaxwellConfig config;
	private MaxwellContext context;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	private void initFirstRun(Connection connection, Connection schemaConnection) throws SQLException, IOException, SchemaSyncError {
		LOGGER.info("Maxwell is capturing initial schema");
		SchemaCapturer capturer = new SchemaCapturer(connection, this.context.getCaseSensitivity());
		this.schema = capturer.capture();

		BinlogPosition pos = BinlogPosition.capture(connection);

		SchemaStore store = new SchemaStore(schemaConnection, this.context.getServerID(), this.schema, pos, this.config.databaseName);

		store.save();

		this.context.setPosition(pos);
	}

	private void run(String[] argv) throws Exception {
		this.config = new MaxwellConfig(argv);

		if ( this.config.log_level != null )
			MaxwellLogging.setLevel(this.config.log_level);

		this.context = new MaxwellContext(this.config);

		this.context.probeConnections();

		try ( Connection connection = this.context.getReplicationConnectionPool().getConnection(); Connection schemaConnection = context.getMaxwellConnectionPool().getConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(schemaConnection);

			SchemaStore.ensureMaxwellSchema(schemaConnection, this.config.databaseName);
			schemaConnection.setCatalog(this.config.databaseName);
			SchemaStore.upgradeSchemaStoreSchema(schemaConnection, this.config.databaseName);

			SchemaStore.handleMasterChange(schemaConnection, context.getServerID(), this.config.databaseName);

			if ( this.context.getInitialPosition() != null ) {
				String producerClass = this.context.getProducer().getClass().getSimpleName();

				LOGGER.info("Maxwell is booting (" + producerClass + "), starting at " + this.context.getInitialPosition());

				SchemaStore store = SchemaStore.restore(schemaConnection, this.context);

				this.schema = store.getSchema();
			} else {
				initFirstRun(connection, schemaConnection);
			}
		} catch ( SQLException e ) {
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		final MaxwellReplicator p = new MaxwellReplicator(this.schema, producer, bootstrapper, this.context, this.context.getInitialPosition());

		bootstrapper.resume(producer, p);

		try {
			p.setFilter(context.buildFilter());
		} catch (MaxwellInvalidFilterException e) {
			LOGGER.error("Invalid maxwell filter", e);
			System.exit(1);
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					p.stopLoop();
				} catch (TimeoutException e) {
					System.err.println("Timed out trying to shutdown maxwell parser thread.");
				}
				context.terminate();
				StaticShutdownCallbackRegistry.invoke();
			}
		});

		this.context.start();
		p.runLoop();

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
