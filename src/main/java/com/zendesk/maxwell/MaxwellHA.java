package com.zendesk.maxwell;

import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RaftLeaderException;
import org.jgroups.protocols.raft.Role;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHA.class);

	private final Maxwell maxwell;
	private final String jgroupsConf, raftMemberID, clientID;
	private boolean hasRun = false;
	private AtomicBoolean isRaftLeader = new AtomicBoolean(false);

	public MaxwellHA(Maxwell maxwell, String jgroupsConf, String raftMemberID, String clientID) {
		this.maxwell = maxwell;
		this.jgroupsConf = jgroupsConf;
		this.raftMemberID = raftMemberID;
		this.clientID = clientID;
	}

	private void run() {
		try {
			if (hasRun)
				maxwell.restart();
			else
				maxwell.start();

			hasRun = true;
		} catch ( Exception e ) {
			LOGGER.error("Maxwell terminating due to exception:", e);
			System.exit(1);
		}
	}

	public void startHA() throws Exception {
		JChannel ch=new JChannel(jgroupsConf);
		RaftHandle handle=new RaftHandle(ch, null);
		if ( raftMemberID != null )
			handle.raftId(raftMemberID);
		else
			LOGGER.warn("--raft_member_id not specified, using values from " + jgroupsConf);

		handle.addRoleListener(role -> {
			if(role == Role.Leader) {
				LOGGER.info("won HA election, starting maxwell");
				isRaftLeader.set(true);

				run();

				isRaftLeader.set(false);
			} else if ( this.isRaftLeader.get() ) {
				LOGGER.info("Unable to find consensus, stepping down HA leadership");
				maxwell.terminate();
				isRaftLeader.set(false);
			} else {
				LOGGER.info("lost HA election, current leader: " + handle.leader());
			}
		});

		ch.connect(this.clientID);
		LOGGER.info("enter HA group, current leader: " +  handle.leader());

		Thread.sleep(Long.MAX_VALUE);
	}
}
