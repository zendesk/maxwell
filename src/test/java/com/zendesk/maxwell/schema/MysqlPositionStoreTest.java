package com.zendesk.maxwell.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;
import com.zendesk.maxwell.*;
import java.sql.ResultSet;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import org.junit.Test;

public class MysqlPositionStoreTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore() throws Exception {
		MaxwellContext context = buildContext();
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), context.getServerID(), "maxwell");
	}

	@Test
	public void testSetBinlogPosition() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new BinlogPosition(12345, "foo"));

		assertThat(buildStore().get(), is(new BinlogPosition(12345, "foo")));
	}

	@Test
	public void testHeartbeat() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new BinlogPosition(12345, "foo"));

		Long preHeartbeat = System.currentTimeMillis();
		store.heartbeat();

		ResultSet rs = server.getConnection().createStatement().executeQuery("select * from maxwell.heartbeats");
		rs.next();

		assertThat(rs.getLong("heartbeat") >= preHeartbeat, is(true));

	}

	@Test
	public void testHeartbeatDuplicate() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new BinlogPosition(12345, "foo"));

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
}
