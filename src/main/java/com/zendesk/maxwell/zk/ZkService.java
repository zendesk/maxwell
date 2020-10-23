package com.zendesk.maxwell.zk;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;


public class ZkService {
	private static final Logger LOGGER = LoggerFactory.getLogger(ZkService.class);
	private static final String rootPath = "/maxwell";
	private static final String masterPath = rootPath + "/master";
	private static final String availablePath = rootPath + "/available";

	private ZkClient zkClient;
	private String thisServer;
	private String thisServerZkPath;

	public ZkService(String zkServers) {
		zkClient = new ZkClient(zkServers, 5000, 5000, new StringSerializer());

		try {
			start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isMaster = false;

	/**
	 * TODO 优化为回调
	 *
	 * @return
	 */
	public boolean isMaster() {
		return isMaster;
	}

	private void start() throws UnknownHostException {
		thisServer = InetAddress.getLocalHost().getHostAddress();
		thisServerZkPath = availablePath + "/" + thisServer;

		LOGGER.info("server: " + thisServer);

		tryToBuildZkFileStructure();

		subscribeAvailableServerChange();

		subscribeSessionStateChange();

		markAvailable();
	}

	private void subscribeSessionStateChange() {
		zkClient.subscribeStateChanges(new IZkStateListener() {
			@Override
			public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
				LOGGER.info("handleStateChanged: " + state);
				try {
					isMaster = false;
					if (state == Watcher.Event.KeeperState.SyncConnected) {
						markAvailable();
					}
				} catch (Exception e) {
					LOGGER.error("handleStateChanged error", e);
				}
			}

			@Override
			public void handleNewSession() throws Exception {
				LOGGER.info("handleNewSession");
			}

			@Override
			public void handleSessionEstablishmentError(Throwable error) throws Exception {
				LOGGER.error("handleSessionEstablishmentError", error);
				isMaster = false;
			}
		});
	}

	private void subscribeAvailableServerChange() {
		zkClient.subscribeChildChanges(availablePath, (parentPath, availableServers) -> {
			LOGGER.info("subscribeChildChanges: " + availableServers);
			try {
				if (availableServers == null || availableServers.isEmpty()) {
					isMaster = false;
					markAvailable();
				} else {
					final Stat oldMasterStat = new Stat();
					final String masterServer = zkClient.readData(masterPath, oldMasterStat);
					if (availableServers.contains(masterServer)) {
						isMaster = StringUtils.equals(thisServer, masterServer);
					} else {
						zkClient.writeData(masterPath, thisServer, oldMasterStat.getVersion());
						isMaster = true;
					}
				}
			} catch (Exception e) {
				LOGGER.error("subscribeChildChanges error", e);
			}
		});
	}


	private void tryToBuildZkFileStructure() {
		try {
			zkClient.createPersistent(masterPath, true);
		} catch (Exception e) {
			LOGGER.error("create masterPath error", e);
		}
		try {
			zkClient.createPersistent(availablePath, true);
		} catch (Exception e) {
			LOGGER.error("create availablePath error", e);
		}
	}


	private void markAvailable() {
		zkClient.delete(thisServerZkPath);
		zkClient.createEphemeral(thisServerZkPath);
	}


}
