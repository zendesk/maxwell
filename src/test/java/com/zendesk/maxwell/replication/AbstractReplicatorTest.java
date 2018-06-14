package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.support.TestReplicator;
import com.zendesk.maxwell.util.RunState;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AbstractReplicatorTest extends MaxwellTestWithIsolatedServer {

	private RowMap heartbeatRow(long ts) {
		Position p = new Position(new BinlogPosition(0L, "binlog-file"), ts);
		return new HeartbeatRowMap("db", p, p);
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
