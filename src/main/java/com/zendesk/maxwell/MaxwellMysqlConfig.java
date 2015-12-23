package com.zendesk.maxwell;

/**
 * Created by kaufmannkr on 12/23/15.
 */
public class MaxwellMysqlConfig {

	public String mysqlHost;
	public Integer mysqlPort;
	public String mysqlUser;
	public String mysqlPassword;

	public MaxwellMysqlConfig() {
		this.mysqlHost = "127.0.0.1";
		this.mysqlPort = 3306;
		this.mysqlUser = "maxwell";
		this.mysqlPassword = null;
	}

	public MaxwellMysqlConfig(String host, Integer port, String user, String password) {
		this.mysqlHost = host;
		this.mysqlPort = port;
		this.mysqlUser = user;
		this.mysqlPassword = password;
	}

	public String getConnectionURI() { return "jdbc:mysql://" + mysqlHost + ":" + mysqlPort;}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() != this.getClass() ) return false;
		MaxwellMysqlConfig other = ((MaxwellMysqlConfig) obj);
		return (this.mysqlHost == other.mysqlHost
				&& this.mysqlPassword == other.mysqlPassword
				&& this.mysqlPort == other.mysqlPort
				&& this.mysqlUser == other.mysqlUser);
	}
}
