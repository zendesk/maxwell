package com.zendesk.maxwell.replication;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class MysqlVersion {
	private final int major;
	private final int minor;

	public MysqlVersion(int major, int minor) {
		this.major = major;
		this.minor = minor;
	}

	public boolean atLeast(int major, int minor) {
		return (this.major > major) || (this.major == major && this.minor >= minor);
	}

	public boolean atLeast(MysqlVersion version) {
		return atLeast(version.major, version.minor);
	}

	public static MysqlVersion capture(Connection c) throws SQLException {
		DatabaseMetaData meta = c.getMetaData();
		return new MysqlVersion(meta.getDatabaseMajorVersion(), meta.getDatabaseMinorVersion());
	}
}
