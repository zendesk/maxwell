package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.schema.SchemaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import snaq.db.ConnectionPool;

public class Recovery {
	static final Logger LOGGER = LoggerFactory.getLogger(Recovery.class);

	private final ConnectionPool replicationConnectionPool;
	private final RecoveryInfo recoveryInfo;
	private final SchemaStore schemaStore;
	private final MaxwellMysqlConfig replicationConfig;
	private final String maxwellDatabaseName;

	public Recovery(MaxwellMysqlConfig replicationConfig,
					String maxwellDatabaseName,
					SchemaStore schemaStore,
					ConnectionPool replicationConnectionPool,
					RecoveryInfo recoveryInfo) {
		this.replicationConfig = replicationConfig;
		this.replicationConnectionPool = replicationConnectionPool;
		this.recoveryInfo = recoveryInfo;
		this.schemaStore = schemaStore;
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	public BinlogPosition recover() throws Exception {
		String recoveryMsg = String.format(
			"old-server-id: %d, file: %s, position: %d, heartbeat: %d",
			recoveryInfo.serverID,
			recoveryInfo.position.getFile(),
			recoveryInfo.position.getOffset(),
			recoveryInfo.heartbeat
		);

		LOGGER.info("attempting to recover from master-change: " + recoveryMsg);

		List<BinlogPosition> list = getBinlogInfo();
		for ( int i = list.size() - 1; i >= 0 ; i-- ) {
			BinlogPosition position = list.get(i);

			LOGGER.debug("scanning binlog: " + position);

			SchemaStore store = schemaStore.clone(recoveryInfo.serverID, recoveryInfo.position, true);
			MaxwellReplicator replicator = new MaxwellReplicator(
				store,
				null,
				null,
				replicationConfig,
				0L, // server-id of 0 activatives "mysqlbinlog" behavior where the server will stop after each binlog
				false,
				null,
				maxwellDatabaseName,
				position,
				true
			);
			BinlogPosition p = findHeartbeat(replicator);
			if ( p != null )
				return p;
		}

		LOGGER.warn("Could not recover from master-change: " + recoveryMsg);
		return null;
	}

	/**
	 * try to find a given heartbeat value from the replicator.
	 * @return A BinlogPosition where the heartbeat was found, or null if none was found.
	 */
	private BinlogPosition findHeartbeat(MaxwellReplicator r) throws Exception {
		r.startReplicator();
		for (RowMap row = r.getRow(); row != null ; row = r.getRow()) {
			if (Objects.equals(r.getLastHeartbeatRead(), recoveryInfo.heartbeat))
				return row.getPosition();

		}
		return null;
	}

	/**
	 * fetch a list of binlog postiions representing the start of each binlog file
	 *
	 * @return a list of binlog positions to attempt recovery at
	 * */

	private List<BinlogPosition> getBinlogInfo() throws SQLException {
		ArrayList<BinlogPosition> list = new ArrayList<>();
		try ( Connection c = replicationConnectionPool.getConnection() ) {
			ResultSet rs = c.createStatement().executeQuery("SHOW BINARY LOGS");
			while ( rs.next() ) {
				list.add(BinlogPosition.at(4, rs.getString("Log_name")));
			}
		}
		return list;
	}
}
