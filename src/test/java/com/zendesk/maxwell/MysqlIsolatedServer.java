package com.zendesk.maxwell;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MysqlIsolatedServer {
	public static final Long SERVER_ID = 4321L;
	private Connection connection; private String baseDir;
	private int port;
	private int serverPid;

	static final Logger LOGGER = LoggerFactory.getLogger(MysqlIsolatedServer.class);
	public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF = new TypeReference<Map<String, Object>>() {};

	public void boot(String xtraParams) throws IOException, SQLException, InterruptedException {
        final String dir = System.getProperty("user.dir");

		if ( xtraParams == null )
			xtraParams = "";

		String serverID = "";
		if ( !xtraParams.contains("--server_id") )
			serverID = "--server_id=" + SERVER_ID;

		ProcessBuilder pb = new ProcessBuilder(
				dir + "/src/test/onetimeserver",
				"--mysql-version=" + this.getVersion(),
				"--log-slave-updates",
				"--log-bin=master",
				"--binlog_format=row",
				"--innodb_flush_log_at_trx_commit=1",
				serverID,
				"--character-set-server=utf8",
				"--default-time-zone=+00:00",
				"--verbose",
				xtraParams
		);

		LOGGER.info("booting onetimeserver: " + StringUtils.join(pb.command(), " "));
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
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> output = mapper.readValue(json, MAP_STRING_OBJECT_REF);
			this.port = (int) output.get("port");
			this.serverPid = (int) output.get("server_pid");
			outputFile = (String) output.get("output");
		} catch ( Exception e ) {
			LOGGER.error("got exception while parsing " + json, e);
			throw(e);
		}


		resetConnection();
		this.connection.createStatement().executeUpdate("GRANT REPLICATION SLAVE on *.* to 'maxwell'@'127.0.0.1' IDENTIFIED BY 'maxwell'");
		this.connection.createStatement().executeUpdate("GRANT ALL on *.* to 'maxwell'@'127.0.0.1'");
		LOGGER.info("booted at port " + this.port + ", outputting to file " + outputFile);
	}

	public void setupSlave(int masterPort) throws SQLException {
		Connection master = DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + masterPort + "/mysql", "root", "");
		ResultSet rs = master.createStatement().executeQuery("show master status");
		if ( !rs.next() )
			throw new RuntimeException("could not get master status");

		String file = rs.getString("File");
		Long position = rs.getLong("Position");

		String changeSQL = String.format(
			"CHANGE MASTER to master_host = '127.0.0.1', master_user='maxwell', master_password='maxwell', "
			+ "master_log_file = '%s', master_log_pos = %d, master_port = %d",
			file, position, masterPort
		);
		getConnection().createStatement().execute(changeSQL);
		getConnection().createStatement().execute("START SLAVE");
	}

	public void boot() throws Exception {
		boot(null);
	}

	public void resetConnection() throws SQLException {
		this.connection = getNewConnection();
	}

	public Connection getNewConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + port + "/mysql?zeroDateTimeBehavior=convertToNull", "root", "");
	}

	public Connection getConnection() {
		return connection;
	}

	public Connection getConnection(String defaultDB) throws SQLException {
		Connection conn = getNewConnection();
		conn.setCatalog(defaultDB);
		return conn;
	}

	public void execute(String query) throws SQLException {
		getConnection().createStatement().executeUpdate(query);
	}

	public void executeList(List<String> queries) throws SQLException {
		for (String q: queries) {
			if ( q.matches("^\\s*$") )
				continue;

			execute(q);
		}
	}

	public void executeList(String[] schemaSQL) throws SQLException {
		executeList(Arrays.asList(schemaSQL));
	}

	public void executeQuery(String sql) throws SQLException {
		getConnection().createStatement().executeUpdate(sql);
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
