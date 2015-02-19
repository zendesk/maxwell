package com.zendesk.maxwell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MysqlIsolatedServer {
	private Connection connection;
	private String baseDir;

	public void boot() throws IOException, SQLException {
        final String dir = System.getProperty("user.dir");

		ProcessBuilder pb = new ProcessBuilder(dir + "/src/test/mysql_isolated_server/bin/boot_isolated_mysql_server", "--", "--binlog_format=row");
		Map<String, String> env = pb.environment();

		env.put("PATH", env.get("PATH") + ":/opt/local/bin");

		Process p = pb.start();
		System.out.println(pb.command());
		InputStream pStdout = p.getInputStream();

		BufferedReader reader = new BufferedReader(new InputStreamReader(pStdout));

		ArrayList<String> mysqlOut = new ArrayList<>();

		while ( true ) {
			String s = reader.readLine();
			if ( s == null || s.equals("UP.") )
				break;
			mysqlOut.add(s);
			System.out.println(s);
		}

		System.out.println(mysqlOut);

		if ( mysqlOut.size() == 0 ) {
			String errLine;
			BufferedReader stderrReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

			while ( (errLine = stderrReader.readLine()) != null )
				System.out.println("mysql isolated error: " + errLine);
		}


		String[] tmpDirSplit = mysqlOut.get(0).split(": ");

		System.out.println(tmpDirSplit[0]);
		this.baseDir = tmpDirSplit[tmpDirSplit.length - 1];

		String[] portSplit = mysqlOut.get(1).split(": ");
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

	public void executeList(String[] schemaSQL) throws SQLException {
		executeList(Arrays.asList(schemaSQL));
	}

	public String getBaseDir() {
		return baseDir;
	}
}
