package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogConnectorLivenessMonitor implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorLivenessMonitor.class);

	private static long heartbeatInterval = 5000L;
	private static final float heartbeatIntervalAllowance = 5.0f;

	private final BinaryLogClient client;
	private long lastEventSeenAt;

	public BinlogConnectorLivenessMonitor(BinaryLogClient client) {
		this.client = client;
		this.client.setHeartbeatInterval(heartbeatInterval);
		this.reset();
	}

	private void reset() {
		this.lastEventSeenAt = System.currentTimeMillis();
	}

	public boolean isAlive() {
		long lastEventAge = System.currentTimeMillis() - lastEventSeenAt;
		long maxAllowedEventAge = Math.round(heartbeatInterval * heartbeatIntervalAllowance);
		boolean alive = lastEventAge <= maxAllowedEventAge;
		if (!alive) {
			LOGGER.warn(
				"Last binlog event seen " + lastEventAge + "ms ago, exceeding " + maxAllowedEventAge + "ms allowance " +
				"(" + heartbeatInterval + " * " + heartbeatIntervalAllowance + ")");
		}
		return alive;
	}

	@Override
	public void onEvent(Event event) {
		this.reset();
	}

	@Override
	public void onConnect(BinaryLogClient binaryLogClient) {
		this.reset();
	}

	@Override
	public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
	}

	@Override
	public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
	}

	@Override
	public void onDisconnect(BinaryLogClient binaryLogClient) {
	}
}
