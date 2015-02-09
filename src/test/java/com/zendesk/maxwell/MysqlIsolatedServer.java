package com.zendesk.maxwell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MysqlIsolatedServer {
	private Connection connection;
	private String baseDir;

	public void boot() throws IOException, SQLException {
        final String dir = System.getProperty("user.dir");

        System.out.println(dir + "/mysql_isolated_server/bin/boot_isolated_mysql_server");
		ProcessBuilder pb = new ProcessBuilder(dir + "/mysql_isolated_server/bin/boot_isolated_mysql_server", "--", "--binlog_format=row");
		Map<String, String> env = pb.environment();

		env.put("PATH", env.get("PATH") + ":/opt/local/bin");

		Process p = pb.start();
		InputStream pStdout = p.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(pStdout));

		String[] tmpDirSplit = reader.readLine().split(": ");
		this.baseDir = tmpDirSplit[tmpDirSplit.length - 1];

		String[] portSplit = reader.readLine().split(": ");
		String port = portSplit[portSplit.length - 1];

		this.connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/mysql", "root", "");
	}

	public Connection getConnection() {
		return connection;
	}

	public void executeList(List<String> queries) throws SQLException {
		for (String q: queries) {
			if ( q.matches("^\\s*$") )
				continue;

			getConnection().createStatement().executeUpdate(q);
		}
	}

	public String getBaseDir() {
		return baseDir;
	}
}
