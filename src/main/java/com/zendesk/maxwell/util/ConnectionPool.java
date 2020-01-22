package com.zendesk.maxwell.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPool {
	Connection getConnection() throws SQLException;
	void release();
}
