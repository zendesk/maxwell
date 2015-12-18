package com.zendesk.maxwell;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class MysqlIsolatedServer {
	public static final Long SERVER_ID = 123123L;
	private Connection connection; private String baseDir;
	private int port;
	private int serverPid;

	static final Logger LOGGER = LoggerFactory.getLogger(MysqlIsolatedServer.class);

	public void boot() throws IOException, SQLException, InterruptedException {
        final String dir = System.getProperty("user.dir");

		ProcessBuilder pb = new ProcessBuilder(
				dir + "/src/test/onetimeserver",
				"--mysql-version=" + this.getVersion(),
				"--log-bin=master",
				"--binlog_format=row",
				"--innodb_flush_log_at_trx_commit=1",
				"--server_id=" + SERVER_ID,
				"--character-set-server=utf8"
		);

		LOGGER.debug("booting onetimeserver: " + StringUtils.join(pb.command(), " "));
		Process p = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

		p.waitFor();

		final BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));

		new Thread() {
			@Override
			public void run() {
				while (true) {
					String l = null;
					try {
						l = errReader.readLine();
					} catch ( IOException e) {};

					if (l == null)
						break;
					System.err.println(l);
				}
			}
		}.start();

		String json = reader.readLine();
		String outputFile = null;
		try {
			JSONObject output = new JSONObject(json);
			this.port = output.getInt("port");
			this.serverPid = output.getInt("server_pid");
			outputFile = output.getString("output");
		} catch ( JSONException e ) {
			LOGGER.error("got exception while parsing " + json, e);
			throw(e);
		}


		resetConnection();
		this.connection.createStatement().executeUpdate("GRANT REPLICATION SLAVE on *.* to 'maxwell'@'127.0.0.1' IDENTIFIED BY 'maxwell'");
		this.connection.createStatement().executeUpdate("GRANT ALL on `maxwell`.* to 'maxwell'@'127.0.0.1'");
		LOGGER.debug("booted at port " + this.port + ", outputting to file " + outputFile);
	}

	public void resetConnection() throws SQLException {
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

	public int getPort() {
		return port;
	}

	public void shutDown() {
		try {
			Runtime.getRuntime().exec("kill " + this.serverPid);
		} catch ( IOException e ) {}
	}

	public String getVersion() {
		String mysqlVersion = System.getenv("MYSQL_VERSION");
		return mysqlVersion == null ? "5.5" : mysqlVersion;

	}
}
