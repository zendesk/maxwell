package com.zendesk.maxwell.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class CuratorUtils {

	private CuratorFramework client;
	private String zookeeperServer;
	private int sessionTimeoutMs;
	private int connectionTimeoutMs;
	private int baseSleepTimeMs;
	private int maxRetries;
	private String namespace = "maxwellHA";
	private String clientId;
	private String electPath;
	private String masterPath;

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

	public void setClientId(String clientId) {
		this.clientId = clientId;
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
				.sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs)
				.namespace(namespace)
				.build();
		client.start();
	}

	public void stop() {
		client.close();
	}

	public CuratorFramework getClient() {
		return client;
	}

	public void register() throws Exception {
		String rootPath = masterPath;
		String hostAddress = InetAddress.getLocalHost().getHostAddress();
		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootPath + "/" + hostAddress);
	}

	public List<String> getChildren(String path) throws Exception {
		List<String> childrenList = new ArrayList<>();
		childrenList = client.getChildren().forPath(path);
		return childrenList;
	}

	public List<String> getInstances() throws Exception {
		return getChildren(masterPath);
	}
}
