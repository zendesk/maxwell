package com.zendesk.maxwell.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class CuratorUtils {
	static final Logger LOGGER = LoggerFactory.getLogger(CuratorUtils.class);
	private CuratorFramework client;
	private String zookeeperServer;
	private int sessionTimeoutMs;
	private int connectionTimeoutMs;
	private int baseSleepTimeMs;
	private int maxRetries;
	private String electPath="/maxwell/services";
	private String masterPath="/maxwell/leader";
	public void setZookeeperServer(String zookeeperServer) {
		this.zookeeperServer = zookeeperServer;
	}
	public String getZookeeperServer() {
		return zookeeperServer;
	}
	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}
	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}
	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}
	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}
	public void setBaseSleepTimeMs(int baseSleepTimeMs) {
		this.baseSleepTimeMs = baseSleepTimeMs;
	}
	public int getBaseSleepTimeMs() {
		return baseSleepTimeMs;
	}
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}
	public int getMaxRetries() {
		return maxRetries;
	}
	public String getElectPath() {
		return electPath;
	}
	public void setElectPath(String electPath) {
		this.electPath = electPath;
	}
	public String getMasterPath() {
		return masterPath;
	}
	public void setMasterPath(String masterPath) {
		this.masterPath = masterPath;
	}

	public void init() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
		client = CuratorFrameworkFactory.builder().connectString(zookeeperServer).retryPolicy(retryPolicy)
				.sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).build();
		client.start();
	}

	public void stop() {
		client.close();
	}
	public CuratorFramework getClient() {
		return client;
	}
	public void register() {
		try {
			String rootPath = masterPath;
			String hostAddress = InetAddress.getLocalHost().getHostAddress();
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootPath + "/" + hostAddress);
		} catch (Exception e) {
			LOGGER.error("register exception", e);
		}
	}

	public List<String> getChildren(String path) {
		List<String> childrenList = new ArrayList<>();
		try {
			childrenList = client.getChildren().forPath(path);
		} catch (Exception e) {
			LOGGER.error("There was an error getting the child nodes", e);
		}
		return childrenList;
	}
	public List<String> getInstances() {
		return getChildren(masterPath);
	}






}
