package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.MaxwellMysqlConfig;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.*;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.zendesk.maxwell.replication.BinlogConnectorReplicator.BINLOG_QUEUE_SIZE;

public class Recovery {
	static final Logger LOGGER = LoggerFactory.getLogger(Recovery.class);

	private final ConnectionPool replicationConnectionPool;
	private final RecoveryInfo recoveryInfo;
	private final MaxwellMysqlConfig replicationConfig;
	private final String maxwellDatabaseName;
	private final RecoverySchemaStore schemaStore;


	public Recovery(MaxwellMysqlConfig replicationConfig,
					String maxwellDatabaseName,
					ConnectionPool replicationConnectionPool,
					CaseSensitivity caseSensitivity,
					RecoveryInfo recoveryInfo) {
		this.replicationConfig = replicationConfig;
		this.replicationConnectionPool = replicationConnectionPool;
		this.recoveryInfo = recoveryInfo;
		this.schemaStore = new RecoverySchemaStore(replicationConnectionPool, maxwellDatabaseName, caseSensitivity);
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	public HeartbeatRowMap recover() throws Exception {
		String recoveryMsg = String.format(
			"old-server-id: %d, position: %s",
			recoveryInfo.serverID,
			recoveryInfo.position
		);

		LOGGER.warn("attempting to recover from master-change: " + recoveryMsg);
		List<BinlogPosition> list = getBinlogInfo();
		for ( int i = list.size() - 1; i >= 0 ; i-- ) {
			BinlogPosition binlogPosition = list.get(i);
			Position position = Position.valueOf(binlogPosition, recoveryInfo.getHeartbeat());
			Metrics metrics = new NoOpMetrics();
			MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
			BinlogConnectorEventProcessor processor = new BinlogConnectorEventProcessor(new TableCache(), schemaStore, position, outputConfig, new RecoveryFilter(this.maxwellDatabaseName), null, new HeartbeatNotifier(), true);

			LOGGER.debug("scanning binlog: {}", binlogPosition);
			Replicator replicator = new BinlogConnectorReplicator(
					null,
					null,
					replicationConfig,
					0L, // server-id of 0 activates "mysqlbinlog" behavior where the server will stop after each binlog
					maxwellDatabaseName,
					metrics,
					position,
					true,
					recoveryInfo.clientID,
					null,
					outputConfig,
					processor,
					0.25f, // Default memory usage size, not used
					1,
					BINLOG_QUEUE_SIZE
			);

			HeartbeatRowMap h = findHeartbeat(replicator);
			if ( h != null ) {
				LOGGER.warn("recovered new master position: " + h.getNextPosition());
				return h;
			}
		}

		LOGGER.error("Could not recover from master-change: " + recoveryMsg);
		return null;
	}

	/**
	 * try to find a given heartbeat value from the replicator.
	 * @return A BinlogPosition where the heartbeat was found, or null if none was found.
	 */
	private HeartbeatRowMap findHeartbeat(Replicator r) throws Exception {
		r.startReplicator();
		for (RowMap row = r.getRow(); row != null ; row = r.getRow()) {
			if (!(row instanceof HeartbeatRowMap)) {
				continue;
			}
			HeartbeatRowMap heartbeatRow = (HeartbeatRowMap) row;
			if (heartbeatRow.getPosition().getLastHeartbeatRead() == recoveryInfo.getHeartbeat())
				return heartbeatRow;
		}
		return null;
	}

	/**
	 * fetch a list of binlog positions representing the start of each binlog file
	 *
	 * @return a list of binlog positions to attempt recovery at
	 * */

	private List<BinlogPosition> getBinlogInfo() throws SQLException {
		ArrayList<BinlogPosition> list = new ArrayList<>();
		try ( Connection c = replicationConnectionPool.getConnection() ;
		      Statement s = c.createStatement();
		      ResultSet rs = s.executeQuery("SHOW BINARY LOGS") ) {
			while ( rs.next() ) {
				list.add(BinlogPosition.at(4, rs.getString("Log_name")));
			}
		}
		return list;
	}
}
