package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.BinlogPosition;
import snaq.db.ConnectionPool;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

/**
 * a schema position object that doesn't write its position out.
 * useful for "replay" mode.
 */
public class ReadOnlySchemaPosition extends SchemaPosition {
	public ReadOnlySchemaPosition(ConnectionPool pool, Long serverID) {
		super(pool, serverID);
	}

	@Override
	public void start() { }

	@Override
	public void stopLoop() throws TimeoutException { }

	@Override
	public void setSync(BinlogPosition p) throws SQLException {
		set(p);
	}
}
