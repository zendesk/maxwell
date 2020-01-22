
package com.zendesk.maxwell.util;

import java.sql.Connection;
import java.sql.SQLException;

public class SnaqConnectionPool extends snaq.db.ConnectionPool implements com.zendesk.maxwell.util.ConnectionPool {
	public SnaqConnectionPool(String name, int maxPool, int maxSize, long idleTimeout, String url, String username, String password) {
		super(name, maxPool, maxSize, idleTimeout, url, username, password);
		this.setCaching(false);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return super.getConnection();
	}

	@Override
	public Connection getConnection(long timeout) throws SQLException {
		return super.getConnection(timeout);
	}
}
