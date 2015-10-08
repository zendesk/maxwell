package com.zendesk.maxwell;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
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
	private MaxwellContext context;
	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	private void initFirstRun(Connection connection) throws SQLException, IOException, SchemaSyncError {
		LOGGER.info("Maxwell is capturing initial schema");
		SchemaCapturer capturer = new SchemaCapturer(connection);
		this.schema = capturer.capture();

		BinlogPosition pos = BinlogPosition.capture(connection);
		SchemaStore store = new SchemaStore(connection, this.context.getServerID(), this.schema, pos);
		store.save();

		this.context.setPosition(pos);
	}

	private void setupFilter(MaxwellFilter filter,MaxwellConfig config){
		if (! config.include_databases.isEmpty()){
			while (config.include_databases.contains(",")) {
				filter.includeDatabase(config.include_databases.substring(0, config.include_databases.indexOf(",")));
				config.include_databases = config.include_databases.substring(config.include_databases.indexOf(",") + 1);
			}
			if (!config.include_databases.isEmpty()) {
				filter.includeDatabase(config.include_databases);
			}
		}
		if (! config.include_tables.isEmpty()){
			while (config.include_tables.contains(",")) {
				filter.includeTable(config.include_tables.substring(0, config.include_tables.indexOf(",")));
				config.include_tables = config.include_tables.substring(config.include_tables.indexOf(",") + 1);
			}
			if (!config.include_tables.isEmpty()) {
				filter.includeTable(config.include_tables);
			}
		}
		
		if (! config.exclude_databases.isEmpty()){
			while (config.exclude_databases.contains(",")) {
				filter.excludeDatabase(config.exclude_databases.substring(0, config.exclude_databases.indexOf(",")));
				config.exclude_databases = config.exclude_databases.substring(config.exclude_databases.indexOf(",") + 1);
			}
			if (!config.exclude_databases.isEmpty()) {
				filter.excludeDatabase(config.exclude_databases);
			}
		}
		if (! config.exclude_tables.isEmpty()){
			while (config.exclude_tables.contains(",")) {
				filter.excludeTable(config.exclude_tables.substring(0, config.exclude_tables.indexOf(",")));
				config.exclude_tables = config.exclude_tables.substring(config.exclude_tables.indexOf(",") + 1);
			}
			if (!config.exclude_tables.isEmpty()) {
				filter.excludeTable(config.exclude_tables);
			}
		}
		
		
	}
	
	private void run(String[] argv) throws Exception {
		this.config = new MaxwellConfig(argv);

		if ( this.config.log_level != null )
			MaxwellLogging.setLevel(this.config.log_level);

		this.context = new MaxwellContext(this.config);

		try ( Connection connection = this.context.getConnectionPool().getConnection() ) {
			MaxwellMysqlStatus.ensureMysqlState(connection);

			SchemaStore.ensureMaxwellSchema(connection);
			SchemaStore.upgradeSchemaStoreSchema(connection);

			SchemaStore.handleMasterChange(connection, context.getServerID());

			if ( this.context.getInitialPosition() != null ) {
				LOGGER.info("Maxwell is booting, starting at " + this.context.getInitialPosition());
				SchemaStore store = SchemaStore.restore(connection, this.context.getServerID(), this.context.getInitialPosition());
				this.schema = store.getSchema();
			} else {
				initFirstRun(connection);
			}
		} catch ( SQLException e ) {
			LOGGER.error("Failed to connect to mysql server @ " + this.config.getConnectionURI());
			LOGGER.error(e.getLocalizedMessage());
			return;
		}

		AbstractProducer producer = this.context.getProducer();

		final MaxwellParser p = new MaxwellParser(this.schema, producer, this.context, this.context.getInitialPosition());
		
		MaxwellFilter filter = new MaxwellFilter();
		setupFilter(filter, config);
		p.setFilter(filter);
		
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
