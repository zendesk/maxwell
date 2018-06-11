package com.zendesk.maxwell.core.replication;

import java.util.Observable;

public class HeartbeatNotifier extends Observable {

	protected void heartbeat(long heartbeat) {
		setChanged();
		notifyObservers(heartbeat);
	}

}
