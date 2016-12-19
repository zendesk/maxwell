package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.bootstrap.AbstractBootstrapper;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.PositionStoreThread;
import com.zendesk.maxwell.schema.SchemaStore;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public abstract class AbstractReplicator extends RunLoopProcess implements Replicator {
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractReplicator.class);
	protected final String clientID;
	protected final PositionStoreThread positionStoreThread;
	protected final AbstractProducer producer;
	protected final AbstractBootstrapper bootstrapper;
	protected final String maxwellSchemaDatabaseName;
	protected final TableCache tableCache = new TableCache();
	protected Long lastHeartbeatRead;

	public AbstractReplicator(String clientID, AbstractBootstrapper bootstrapper, PositionStoreThread positionStoreThread, String maxwellSchemaDatabaseName, AbstractProducer producer) {
		this.clientID = clientID;
		this.bootstrapper = bootstrapper;
		this.positionStoreThread = positionStoreThread;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.producer = producer;
	}

	/**
	 * Possibly convert a RowMap object into a HeartbeatRowMap
	 *
	 * Process a rowmap that represents a write to `maxwell`.`heartbeats`.
	 * If it's a write for a different client_id, we return the input (which
	 * will signify to the rest of the chain to ignore it).  Otherwise, we
	 * transform it into a HeartbeatRowMap (which will not be output, but will
	 * advance the binlog position) and set `this.lastHeartbeatRead`
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	protected RowMap processHeartbeats(RowMap row) throws SQLException {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row; // plain row -- do not process.

		this.lastHeartbeatRead = (Long) row.getData("heartbeat");
		LOGGER.debug("replicator picked up heartbeat: " + this.lastHeartbeatRead);
		return HeartbeatRowMap.valueOf(row.getDatabase(), row.getPosition(), this.lastHeartbeatRead);
	}

	/**
	 * Parse a DDL statement and output the results to the producer
	 *
	 * @param dbName The database "context" under which the SQL is to be processed.  think "use db; alter table foo ..."
	 * @param sql The DDL SQL to be processed
	 * @param schemaStore A SchemaStore object to which we delegate the parsing of the sql
	 * @param position The position that the SQL happened at
	 * @param timestmap The timestamp of the SQL binlog event
	 */
	protected void processQueryEvent(String dbName, String sql, SchemaStore schemaStore, BinlogPosition position, Long timestamp) throws Exception {
		List<ResolvedSchemaChange> changes =  schemaStore.processSQL(sql, dbName, position);
		for ( ResolvedSchemaChange change : changes ) {
			DDLMap ddl = new DDLMap(change, timestamp, sql, position);
			producer.push(ddl);
		}

		tableCache.clear();

		if ( this.producer != null )
			this.producer.writePosition(position);
	}

	/**
	 * Should we output an event for the given database and table?
	 *
	 * Here we check against a whitelist/blacklist/filter.  The whitelist
	 * passes updates to `maxwell.bootstrap` through (those are control
	 * mechanisms for bootstrap), the blacklist gets rid of the
	 * `ha_health_check` table which shows up oddly in Amazon RDS.
	 *
	 * @param database The database of the DML
	 * @param table The table of the DML
	 * @param filter A table-filter, or null
	 * @return Whether we should write the event to the producer
	 */
	protected boolean shouldOutputEvent(String database, String table, MaxwellFilter filter) {
		Boolean isSystemWhitelisted = database.equals(this.maxwellSchemaDatabaseName)
			&& table.equals("bootstrap");

		Boolean isSystemBlacklisted = database.equals("mysql") && table.equals("ha_health_check");
		if ( isSystemBlacklisted )
			return false;
		else if ( isSystemWhitelisted)
			return true;
		else
			return (filter == null || filter.matches(database, table));
	}

	/**
	 * Get the last heartbeat that the replicator has processed.
	 *
	 * We pass along the value of the heartbeat to the producer inside the row map.
	 * @return the millisecond value ot the last heartbeat read
	 */

	public Long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}

	/**
	 * get a single row from the replicator and pass it to the producer or bootstrapper.
	 *
	 * This is the top-level function in the run-loop.
	 */
	public void work() throws Exception {
		RowMap row = getRow();

		// todo: this is inelegant.  Ideally the outer code would monitor the
		// position thread and stop us if it was dead.

		if ( positionStoreThread.getException() != null )
			throw positionStoreThread.getException();

		if ( row == null )
			return;

		if ( row instanceof HeartbeatRowMap)
			producer.push(row);
		else if (!bootstrapper.shouldSkip(row) && !isMaxwellRow(row))
			producer.push(row);
		else
			bootstrapper.work(row, producer, this);
	}

	/**
	 * Is this RowMap an update to one of maxwell's own tables?
	 *
	 * If so we will often suppress the output.
	 * @param row The RowMap in question
	 * @return whether the update is something maxwell itself generated
	 */
	protected boolean isMaxwellRow(RowMap row) {
		return row.getDatabase().equals(this.maxwellSchemaDatabaseName);
	}

	/**
	 * The main entry point into the event reading loop.
	 *
	 * We maintain a buffer of events in a transaction,
	 * and each subsequent call to `getRow` can grab one from
	 * the buffer.  If that buffer is empty, we'll go check
	 * the open-replicator buffer for rows to process.  If that
	 * buffer is empty, we return null.
	 *
	 * @return either a RowMap or null
	 */
	public abstract RowMap getRow() throws Exception;
}
