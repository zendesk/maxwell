package com.zendesk.maxwell.core.schema;

import com.zendesk.maxwell.api.replication.Position;
import snaq.db.ConnectionPool;

/**
 * a schema position object that doesn't write its position out.
 * useful for "replay" mode.
 */
public class ReadOnlyMysqlPositionStore extends MysqlPositionStore {
	public ReadOnlyMysqlPositionStore(ConnectionPool pool, Long serverID, String clientID, boolean gtidMode) {
		super(pool, serverID, clientID, gtidMode);
	}

	@Override
	public void set(Position p) { }

	@Override
	public long heartbeat() throws Exception {
		return System.currentTimeMillis();
	}
}
