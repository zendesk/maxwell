package com.zendesk.maxwell.schema;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;
import com.zendesk.maxwell.*;
import java.sql.ResultSet;
import java.util.List;

import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.replication.Position;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class MysqlPositionStoreTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore() throws Exception {
		return buildStore(buildContext());
	}

	private MysqlPositionStore buildStore(MaxwellContext context) throws Exception {
		return buildStore(context, context.getServerID());
	}

	private MysqlPositionStore buildStore(MaxwellContext context, Long serverID) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), serverID, "maxwell", MaxwellTestSupport.inGtidMode());
	}

	private MysqlPositionStore buildStore(MaxwellContext context, Long serverID, String clientId) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), serverID, clientId, MaxwellTestSupport.inGtidMode());
	}

	@Test
	public void testSetBinlogPosition() throws Exception {
		MysqlPositionStore store = buildStore();
		long lastHeartbeatRead = 100L;
		BinlogPosition binlogPosition;
		if (MaxwellTestSupport.inGtidMode()) {
			String gtid = "123:1-100";
			binlogPosition = new BinlogPosition(gtid, null, 12345, "foo");
		} else {
			binlogPosition = new BinlogPosition(12345, "foo");
		}
		Position position = new Position(binlogPosition, 100L);
		store.set(position);
		assertThat(buildStore().get(), is(position));
	}

	@Test
	public void testHeartbeat() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new Position(new BinlogPosition(12345, "foo"), 0L));

		Long preHeartbeat = System.currentTimeMillis();
		store.heartbeat();

		ResultSet rs = server.getConnection().createStatement().executeQuery("select * from maxwell.heartbeats");
		rs.next();

		assertThat(rs.getLong("heartbeat") >= preHeartbeat, is(true));
	}

	@Test
	public void testHeartbeatDuplicate() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new Position(new BinlogPosition(12345, "foo"), 0L));

		store.heartbeat();
		buildStore().heartbeat();


		Exception exception = null;

		try {
			store.heartbeat();
		} catch (DuplicateProcessException d) {
			exception = d;
		}

		assertThat(exception, is(not(nullValue())));
	}

	@Test
	public void testEmptyPositionRecovery() throws Exception {
		MaxwellContext context = buildContext();
		MysqlPositionStore store = buildStore(context);
		List<RecoveryInfo> recoveries = store.getAllRecoveryInfos();

		assertThat(recoveries.size(), is(0));

		String errorMessage = StringUtils.join(store.formatRecoveryFailure(context.getConfig(), recoveries), "\n");
		assertThat(errorMessage, is("Unable to find any binlog positions in `positions` table"));
		assertThat(store.getRecoveryInfo(context.getConfig()), is(nullValue()));
	}

	@Test
	public void testMultiplePositionRecovery() throws Exception {
		MaxwellContext context = buildContext();
		Long activeServerID = context.getServerID();

		Long newestServerID = activeServerID + 1;
		Long intermediateServerID = activeServerID + 2;
		Long oldestServerID = activeServerID + 3;

		Long newestHeartbeat = 123L;
		Long intermediateHeartbeat = newestHeartbeat - 10;
		Long oldestHeartbeat = newestHeartbeat - 20;
		String binlogFile = "bin.log";

		buildStore(context, oldestServerID).set(new Position(new BinlogPosition(0L, binlogFile), oldestHeartbeat));
		buildStore(context, intermediateServerID).set(new Position(new BinlogPosition(0L, binlogFile), intermediateHeartbeat));
		buildStore(context, newestServerID).set(new Position(new BinlogPosition(0L, binlogFile), newestHeartbeat));
		MysqlPositionStore store = buildStore(context);

		List<RecoveryInfo> recoveries = store.getAllRecoveryInfos();

		if (MaxwellTestSupport.inGtidMode()) {
			assertThat(recoveries.size(), is(1));
			// gtid mode can't get into a multiple recovery state
			return;
		}

		assertThat(recoveries.size(), is(3));
		assertThat(store.getRecoveryInfo(context.getConfig()), is(nullValue()));

		String errorMessage = StringUtils.join(store.formatRecoveryFailure(context.getConfig(), recoveries), "\n");
		assertThat(errorMessage, containsString("Found multiple binlog positions for cluster in `positions` table."));
		for (RecoveryInfo recovery: recoveries) {
			assertThat(errorMessage, containsString(" - " + recovery));
		}
		assertThat(errorMessage, containsString("execute: DELETE FROM maxwell.positions WHERE server_id <> " + newestServerID + " AND client_id = '<your_client_id>';"));
	}

	@Test
	public void testCleanupOldRecoveryInfos() throws Exception {
		if (MaxwellTestSupport.inGtidMode()) {
			// gtid mode can't get into a multiple recovery state
			return;
		}

		MaxwellContext context = buildContext();
		Long activeServerID = context.getServerID();
		Long oldServerID1 = activeServerID + 1;
		Long oldServerID2 = activeServerID + 2;

		String binlogFile = "bin.log";
		String clientId = "client-123";

		buildStore(context, oldServerID1, clientId).set(new Position(new BinlogPosition(0L, binlogFile), 1L));
		buildStore(context, oldServerID2, clientId).set(new Position(new BinlogPosition(0L, binlogFile), 2L));
		MysqlPositionStore testStore = buildStore(context, activeServerID, clientId);
		testStore.set(new Position(new BinlogPosition(0L, binlogFile), 3L));

		assertEquals(3, testStore.getAllRecoveryInfos().size());
		testStore.cleanupOldRecoveryInfos();
		assertEquals(1, testStore.getAllRecoveryInfos().size());
	}
}
