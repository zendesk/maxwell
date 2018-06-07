package com.zendesk.maxwell.core.replication;

import com.zendesk.maxwell.api.replication.BinlogPosition;
import com.zendesk.maxwell.api.replication.Position;
import com.zendesk.maxwell.api.row.RowMap;
import com.zendesk.maxwell.core.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.core.row.HeartbeatRowMap;
import com.zendesk.maxwell.core.support.TestReplicator;
import com.zendesk.maxwell.api.RunState;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AbstractReplicatorTest extends MaxwellTestWithIsolatedServer {

	private RowMap heartbeatRow(long ts) {
		return new HeartbeatRowMap("db", new Position(new BinlogPosition(0L, "binlog-file"), ts));
	}

	@Test
	public void testStopsAfterTargetHeartbeatReceived() throws Exception {
		TestReplicator replicator = new TestReplicator(buildContext());
		replicator.stopAtHeartbeat(2L);

		replicator.processRow(heartbeatRow(1L));
		assertThat(replicator.getState(), is(RunState.RUNNING));

		replicator.processRow(heartbeatRow(2L));
		assertThat(replicator.getState(), is(RunState.STOPPED));
	}
}
