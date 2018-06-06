package com.zendesk.maxwell.core.schema;

import com.zendesk.maxwell.core.MaxwellSystemContext;
import com.zendesk.maxwell.core.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.core.support.MaxwellTestSupport;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class PositionStoreThreadTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore(MaxwellSystemContext context) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), context.getServerID(), "maxwell", MaxwellTestSupport.inGtidMode());
	}

	@Test
	public void testStoresFinalPosition() throws Exception {
		MaxwellSystemContext context = buildContext();
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
		MaxwellSystemContext context = buildContext();
		MysqlPositionStore store = buildStore(context);
		Position initialPosition = new Position(new BinlogPosition(4L, "file"), 0L);
		PositionStoreThread thread = new PositionStoreThread(store, context);

		thread.setPosition(initialPosition);
		thread.storeFinalPosition();

		assertThat(store.get(), nullValue());
	}
}
