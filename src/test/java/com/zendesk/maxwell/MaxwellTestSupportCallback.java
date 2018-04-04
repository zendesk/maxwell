package com.zendesk.maxwell;

import com.zendesk.maxwell.replication.Position;

import java.sql.SQLException;

import static com.zendesk.maxwell.MaxwellTestSupport.inGtidMode;

public class MaxwellTestSupportCallback {
	public void beforeReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {}

	public void beforeReplicatorStart(MysqlIsolatedServer mysql, MaxwellConfig mutableConfig) throws SQLException {
		beforeReplicatorStart(mysql);
		mutableConfig.initPosition = Position.capture(mysql.getConnection(), inGtidMode());
	}

	public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {}
	public void beforeTerminate(MysqlIsolatedServer mysql) { }
}
