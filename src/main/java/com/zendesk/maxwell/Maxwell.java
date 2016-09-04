package com.zendesk.maxwell;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import com.djdch.log4j.StaticShutdownCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreSchema;

public class Maxwell implements Runnable {
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected MaxwellReplicator replicator;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) throws SQLException {
		this.config = config;
		this.context = new MaxwellContext(this.config);
		this.context.probeConnections();
	}

	public void run() {
		try {
			start();
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	public void terminate() {
		try {
			// send a final heartbeat through the system
			context.heartbeat();
			Thread.sleep(100);

			if ( this.replicator != null)
				replicator.stopLoop();
			} catch (TimeoutException e) {
				System.err.println("Timed out trying to shutdown maxwell parser thread.");
			} catch (InterruptedException e) {
			} catch (Exception e) {
		}

		if ( this.context != null )
			context.terminate();

		replicator = null;
		context = null;
	}

	public String getMaxwellVersion() {
		String packageVersion = getClass().getPackage().getImplementationVersion();
		if ( packageVersion == null )
			return "??";
		else
			return packageVersion;
	}

	static String bootString =
		"Maxwell v%s is booting (%s), starting at %s";

	private void start() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
			  Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);

			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
				SchemaStoreSchema.handleMasterChange(schemaConnection, context.getServerID());
			}

			String producerClass = this.context.getProducer().getClass().getSimpleName();
			LOGGER.info(String.format(bootString, getMaxwellVersion(), producerClass, this.context.getInitialPosition().toString()));
		} catch ( SQLException e ) {
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.context.getProducer();
		AbstractBootstrapper bootstrapper = this.context.getBootstrapper();

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, this.context.getInitialPosition());
		this.replicator = new MaxwellReplicator(mysqlSchemaStore, producer, bootstrapper, this.context, this.context.getInitialPosition());

		bootstrapper.resume(producer, replicator);

		replicator.setFilter(context.getFilter());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				terminate();
			}
		});

		this.context.start();
		replicator.runLoop();
	}

	public static void main(String[] args) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				StaticShutdownCallbackRegistry.invoke();
			}
		});

		try {
			MaxwellConfig config = new MaxwellConfig(args);

			if ( config.log_level != null )
				MaxwellLogging.setLevel(config.log_level);

			new Maxwell(config).start();
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
