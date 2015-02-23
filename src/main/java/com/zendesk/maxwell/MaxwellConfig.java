package com.zendesk.maxwell;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MaxwellConfig {
	public String mysqlHost;
	public int    mysqlPort;
	public String mysqlUser;
	public String mysqlPassword;

	public String currentPositionFile;

	private BinlogPosition initialPosition;

	public Connection getMasterConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:mysql://" + mysqlHost + ":" + mysqlPort, mysqlUser, mysqlPassword);
	}

	public BinlogPosition getInitialPosition() throws FileNotFoundException, IOException {
		if ( this.initialPosition != null )
			return this.initialPosition;

		File f = new File(this.currentPositionFile);
		if ( !f.exists() ) {
			return null;
		} else {
			Properties p = new Properties();
			p.load(new FileReader(f));

			this.initialPosition = new BinlogPosition(Integer.valueOf((String) p.get("offset")), p.getProperty("file"));
			return this.initialPosition;
		}
	}

	public void setInitialPosition(BinlogPosition position) throws IOException {
		Properties p = new Properties();
		p.setProperty("offset", String.valueOf(position.getOffset()));
		p.setProperty("file", position.getFile());

		File f = new File(this.currentPositionFile);
		FileWriter fw = new FileWriter(f);
		try {
			p.store(fw, "");
			this.initialPosition = position;
		} finally {
			fw.close();
		}
	}

	public static MaxwellConfig fromPropfile(String filename) throws IOException {
		Properties p = new Properties();
		FileReader reader = new FileReader(new File(filename));
		p.load(reader);

		MaxwellConfig config = new MaxwellConfig();

		config.mysqlHost     = p.getProperty("host", "127.0.0.1");
		config.mysqlPassword = p.getProperty("password");
		config.mysqlUser     = p.getProperty("user");
		config.mysqlPort     = Integer.valueOf(p.getProperty("port", "3306"));

		config.currentPositionFile = p.getProperty("position_file", "maxwell.position");

		System.out.println(config.mysqlPassword);
		return config;
	}
}
