package com.zendesk.maxwell;

import com.zendesk.maxwell.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.RaftHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class that joins a jgroups-raft cluster of servers or zookeeper
 */
public class MaxwellHA {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellHA.class);

	private final Maxwell maxwell;
	private String jgroupsConf, raftMemberID, clientID;
	private String zookeeperServer;
	private int sessionTimeoutMs, connectionTimeoutMs, maxRetries, baseSleepTimeMs;
	private boolean hasRun = false;
	private AtomicBoolean isRaftLeader = new AtomicBoolean(false);

	/**
	 * Build a MaxwellHA object
	 * @param maxwell The Maxwell instance that will be run when an election is won
	 * @param jgroupsConf Path to an xml file that will configure the RAFT cluster
	 * @param raftMemberID unique ID identifying the raft member in the cluster
	 * @param clientID The maxwell clientID.  Used to create a unique "channel" for the election
	 */
	public MaxwellHA(Maxwell maxwell, String jgroupsConf, String raftMemberID, String clientID) {
		this.maxwell = maxwell;
		this.jgroupsConf = jgroupsConf;
		this.raftMemberID = raftMemberID;
		this.clientID = clientID;
	}

	/**
	 * Build a MaxwellHA object
	 * @param maxwell The Maxwell instance that will be run when an election is won
	 * @param zookeeperServer zookeeper adds
	 * @param sessionTimeoutMs
	 * @param connectionTimeoutMs
	 * @param maxRetries
	 * @param baseSleepTimeMs
	 * @param clientID The maxwell clientID. This will be the only one through which the actual path is stored
	 */
	public MaxwellHA(Maxwell maxwell, String zookeeperServer, int sessionTimeoutMs, int connectionTimeoutMs, int maxRetries, int baseSleepTimeMs, String clientID) {
		this.maxwell = maxwell;
		this.zookeeperServer = zookeeperServer;
		this.sessionTimeoutMs = sessionTimeoutMs;
		this.connectionTimeoutMs = connectionTimeoutMs;
		this.maxRetries = maxRetries;
		this.baseSleepTimeMs = baseSleepTimeMs;
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

	/**
	 * Join the raft cluster, starting and stopping Maxwell on elections.
	 *
	 * Does not return.
	 * @throws Exception if there's any issues
	 */
	public void startHAJGroups() throws Exception {
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

	/**
	 * indicates that Ha is started in zookeeper mode
	 * @throws Exception
	 */
	public void startHAZookeeper() throws Exception {

		Lock lock = new ReentrantLock();
		String hostAddress = InetAddress.getLocalHost().getHostAddress();

		String electPath = "/" + clientID + "/services";
		String masterPath = "/" + clientID + "/leader";
		CuratorUtils cu = new CuratorUtils();
		cu.setZookeeperServer(zookeeperServer);
		cu.setSessionTimeoutMs(sessionTimeoutMs);
		cu.setConnectionTimeoutMs(connectionTimeoutMs);
		cu.setMaxRetries(maxRetries);
		cu.setBaseSleepTimeMs(baseSleepTimeMs);
		cu.setClientId(clientID);
		cu.setElectPath(electPath);
		cu.setMasterPath(masterPath);
		cu.init();
		CuratorFramework client = cu.getClient();
		LeaderLatch leader = new LeaderLatch(client, cu.getElectPath(),hostAddress,LeaderLatch.CloseMode.NOTIFY_LEADER);
		leader.start();
		LOGGER.info("this node:" + hostAddress + " is participating in the election of the leader ....");
		leader.addListener(new LeaderLatchListener() {
			@Override
			public void isLeader() {
				try {
					lock.lock();
					cu.register();
				} catch (Exception e) {
					e.printStackTrace();
					LOGGER.error("The node registration is abnormal, check whether the maxwell host communicates properly with the zookeeper network");
					cu.stop();
					System.exit(1);
				}finally {
					lock.unlock();
				}
				LOGGER.info("node:" + hostAddress + " is current leader, starting Maxwell....");
				LOGGER.info("hasLeadership = " + leader.hasLeadership());

				run();

				try {
					leader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				cu.stop();
			}

			@Override
			public void notLeader() {
				try {
					lock.lock();
					LOGGER.warn("node:" + hostAddress + " lost leader");
					LOGGER.warn("master-slave switchover......");
					LOGGER.warn("The leadership went from " + hostAddress + " to " + leader.getLeader());
				}catch (Exception e){
					e.printStackTrace();
				}finally {
					lock.unlock();
				}
			}
		});

		Thread.sleep(Long.MAX_VALUE);
	}

}
