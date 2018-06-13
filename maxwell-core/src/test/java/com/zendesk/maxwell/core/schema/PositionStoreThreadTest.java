package com.zendesk.maxwell.core.schema;

import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.core.util.test.mysql.MysqlIsolatedServer;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class PositionStoreThreadTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore(MaxwellContext context) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), context.getServerID(), "maxwell", MysqlIsolatedServer.inGtidMode());
	}

	@Test
	public void testStoresFinalPosition() throws Exception {
		MaxwellContext context = buildContext();
		MysqlPositionStore store = buildStore(context);
		Position initialPosition = new Position(new BinlogPosition(4L, "file"), 0L);
		Position finalPosition = new Position(new BinlogPosition(88L, "file"), 1L);
		PositionStoreThread thread = new PositionStoreThread(store, context);

		thread.setPosition(initialPosition);
		thread.setPosition(finalPosition);
		thread.storeFinalPosition();

		assertThat(store.get(), is(finalPosition));
	}

	@Test
	public void testDoesNotStoreUnchangedPosition() throws Exception {
		MaxwellContext context = buildContext();
		MysqlPositionStore store = buildStore(context);
		Position initialPosition = new Position(new BinlogPosition(4L, "file"), 0L);
		PositionStoreThread thread = new PositionStoreThread(store, context);

		thread.setPosition(initialPosition);
		thread.storeFinalPosition();

		assertThat(store.get(), nullValue());
	}
}
