package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.*;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.MaxwellReplicator;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.RowMap;
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
	private final MaxwellMysqlConfig replicationConfig;
	private final String maxwellDatabaseName;
	private final RecoverySchemaStore schemaStore;
	private final boolean shykoMode;

	public Recovery(MaxwellMysqlConfig replicationConfig,
					String maxwellDatabaseName,
					ConnectionPool replicationConnectionPool,
					CaseSensitivity caseSensitivity,
					RecoveryInfo recoveryInfo,
					boolean shykoMode) {
		this.replicationConfig = replicationConfig;
		this.replicationConnectionPool = replicationConnectionPool;
		this.recoveryInfo = recoveryInfo;
		this.schemaStore = new RecoverySchemaStore(replicationConnectionPool, maxwellDatabaseName, caseSensitivity);
		this.maxwellDatabaseName = maxwellDatabaseName;
		this.shykoMode = shykoMode;
	}

	public BinlogPosition recover() throws Exception {
		String recoveryMsg = String.format(
			"old-server-id: %d, file: %s, position: %d, heartbeat: %d",
			recoveryInfo.serverID,
			recoveryInfo.position.getFile(),
			recoveryInfo.position.getOffset(),
			recoveryInfo.heartbeat
		);

		LOGGER.warn("attempting to recover from master-change: " + recoveryMsg);

		List<BinlogPosition> list = getBinlogInfo();
		for ( int i = list.size() - 1; i >= 0 ; i-- ) {
			BinlogPosition position = list.get(i);

			LOGGER.debug("scanning binlog: " + position);
			Replicator replicator;
			if ( shykoMode ) {
				replicator = new BinlogConnectorReplicator(
						this.schemaStore,
						null,
						null,
						replicationConfig,
						0L, // server-id of 0 activates "mysqlbinlog" behavior where the server will stop after each binlog
						maxwellDatabaseName,
						position,
						true,
						recoveryInfo.clientID
						);
			} else {
				replicator = new MaxwellReplicator(
						this.schemaStore,
						null,
						null,
						replicationConfig,
						0L, // server-id of 0 activates "mysqlbinlog" behavior where the server will stop after each binlog
						false,
						maxwellDatabaseName,
						position,
						true,
						recoveryInfo.clientID
						);
			}

			replicator.setFilter(new RecoveryFilter(this.maxwellDatabaseName));

			BinlogPosition p = findHeartbeat(replicator);
			if ( p != null ) {
				LOGGER.warn("recovered new master position: " + p);
				return p;
			}
		}

		LOGGER.error("Could not recover from master-change: " + recoveryMsg);
		return null;
	}

	/**
	 * try to find a given heartbeat value from the replicator.
	 * @return A BinlogPosition where the heartbeat was found, or null if none was found.
	 */
	private BinlogPosition findHeartbeat(Replicator r) throws Exception {
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
