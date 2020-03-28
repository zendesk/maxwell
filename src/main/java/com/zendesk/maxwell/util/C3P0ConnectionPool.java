package com.zendesk.maxwell.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

public class C3P0ConnectionPool implements ConnectionPool {
	private final ComboPooledDataSource cpds;

	@Override
	public Connection getConnection() throws SQLException {
		return cpds.getConnection();
	}

	@Override
	public void release() {
		cpds.close();
	}

	public C3P0ConnectionPool(String url, String user, String password) {
		cpds = new ComboPooledDataSource();
		cpds.setJdbcUrl(url);
		cpds.setUser(user);
		cpds.setPassword(password);


		// the settings below are optional -- c3p0 can work with defaults
		cpds.setMinPoolSize(1);
		cpds.setMaxPoolSize(5);
	}
}
