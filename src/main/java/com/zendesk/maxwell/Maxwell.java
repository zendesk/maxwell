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
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class Maxwell {
	private MaxwellConfig config;
	private MaxwellContext context;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	private void initFirstRun(Connection connection, Connection schemaConnection) throws SQLException, IOException, InvalidSchemaError {
		BinlogPosition pos = BinlogPosition.capture(connection);
		this.context.setPosition(pos);
	}

	private void run(String[] argv) throws Exception {
		this.config = new MaxwellConfig(argv);

		if ( this.config.log_level != null )
			MaxwellLogging.setLevel(this.config.log_level);

		this.context = new MaxwellContext(this.config);

		this.context.probeConnections();

		try ( Connection connection = this.context.getReplicationConnection(); Connection schemaConnection = context.getMaxwellConnectionPool().getConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(schemaConnection);

			SchemaStoreSchema.ensureMaxwellSchema(schemaConnection, this.config.databaseName);
			schemaConnection.setCatalog(this.config.databaseName);
			SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection, this.config.databaseName);

			SchemaStoreSchema.handleMasterChange(schemaConnection, context.getServerID(), this.config.databaseName);

			if ( this.context.getInitialPosition() == null ) {
				initFirstRun(connection, schemaConnection);
			}

			String producerClass = this.context.getProducer().getClass().getSimpleName();
			LOGGER.info("Maxwell is booting (" + producerClass + "), starting at " + this.context.getInitialPosition());
		} catch ( SQLException e ) {
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context);
		final MaxwellReplicator p = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, this.context.getInitialPosition());

		bootstrapper.resume(producer, p);

		p.setFilter(context.getFilter());

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
