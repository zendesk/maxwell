package com.zendesk.maxwell;

import java.sql.SQLException;

public class MaxwellTestSupportCallback {
	public void beforeReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {}
	public void afterReplicatorStart(MysqlIsolatedServer mysql) throws SQLException {}
	public void beforeTerminate(MysqlIsolatedServer mysql) { }
}
