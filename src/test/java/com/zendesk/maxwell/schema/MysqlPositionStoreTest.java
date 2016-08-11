package com.zendesk.maxwell.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import com.zendesk.maxwell.*;
import org.junit.Test;

public class MysqlPositionStoreTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore() throws Exception {
		MaxwellContext context = buildContext();
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), context.getServerID(), "maxwell", "maxwell");
	}

	@Test
	public void TestSetBinlogPosition() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new BinlogPosition(12345, "foo"));

		assertThat(buildStore().get(), is(new BinlogPosition(12345, "foo")));
	}
}
