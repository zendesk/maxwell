
package com.zendesk.maxwell.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class SnaqConnectionPool extends snaq.db.ConnectionPool implements com.zendesk.maxwell.util.ConnectionPool {
	public SnaqConnectionPool(String name, int minPool, int maxPool, int maxSize, long idleTimeout, String url, String username, String password) {
		super(name, minPool, maxPool, maxSize, idleTimeout, url, username, password);
		this.setCaching(false);
	}

	public SnaqConnectionPool(String name, int maxPool, int maxSize, long idleTimeout, String url, String username, String password) {
		super(name, maxPool, maxSize, idleTimeout, url, username, password);
	}

	public SnaqConnectionPool(String name, int minPool, int maxPool, int maxSize, long idleTimeout, String url, Properties props) {
		super(name, minPool, maxPool, maxSize, idleTimeout, url, props);
	}

	public SnaqConnectionPool(String name, int maxPool, int maxSize, long idleTimeout, String url, Properties props) {
		super(name, maxPool, maxSize, idleTimeout, url, props);
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
