package com.zendesk.maxwell.replication;

import org.apache.arrow.flatbuf.Int;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class MysqlVersion {
	private final int major;
	private final int minor;
	public boolean isMariaDB = false;

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

	public boolean lessThan(int major, int minor) {
		return (this.major < major) || (this.major == major & this.minor < minor);
	}

	public static MysqlVersion capture(Connection c) throws SQLException {
		DatabaseMetaData meta = c.getMetaData();
		return new MysqlVersion(meta.getDatabaseMajorVersion(), meta.getDatabaseMinorVersion());
	}

	public int getMajor() {
		return this.major;
	}

	public int getMinor() {
		return this.minor;
	}

	public static MysqlVersion parse(String versionString) {
		if ( versionString.equals("mariadb") ) {
			MysqlVersion v = new MysqlVersion(0, 0);
			v.isMariaDB = true;
			return v;
		}


		String[] split = versionString.split("\\.");
		if ( split.length < 2 ) {
			throw new IllegalArgumentException("Invalid version string: " + versionString + ". Expected at least major and minor versions separated by a dot.");
		}
		return new MysqlVersion(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

}
