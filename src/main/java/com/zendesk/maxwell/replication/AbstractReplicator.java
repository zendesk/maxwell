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
	 * Process a rowmap that represents a write to `maxwell`.`positions`.
	 * If it's a write for a different client_id, or it's not a heartbeat (a position set),
	 * we return just the RowMap.  Otherwise, we transform it into a HeartbeatRowMap
	 * and set lastHeartbeatRead.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */
	protected RowMap processHeartbeats(RowMap row) throws SQLException {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row;

		Object heartbeat_at = row.getData("heartbeat_at");
		Object old_heartbeat_at = row.getOldData("heartbeat_at"); // make sure it's a heartbeat update, not a position set.

		if ( heartbeat_at != null && old_heartbeat_at != null ) {
			Long thisHeartbeat = (Long) heartbeat_at;
			if ( !thisHeartbeat.equals(lastHeartbeatRead) ) {
				LOGGER.debug("prcessing heartbeat: " + thisHeartbeat + " @" + row.getPosition());
				this.lastHeartbeatRead = thisHeartbeat;

				return HeartbeatRowMap.valueOf(row.getDatabase(), row.getPosition(), thisHeartbeat);
			}
		}
		return row;
	}

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

	protected boolean shouldOutputEvent(String database, String table, MaxwellFilter filter) {
		/* always pass bootstrap rows through */
		Boolean isSystemWhitelisted = database.equals(this.maxwellSchemaDatabaseName)
			&& table.equals("bootstrap");

		/* there's an odd RDS thing, I guess, where ha_health_check doesn't
		 * show up in INFORMATION_SCHEMA but it's replicated nonetheless. */
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
	 * @return the millisecond value ot fhte last heartbeat
	 */

	public Long getLastHeartbeatRead() {
		return lastHeartbeatRead;
	}

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
