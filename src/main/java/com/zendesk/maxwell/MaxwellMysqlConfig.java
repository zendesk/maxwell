package com.zendesk.maxwell;

import joptsimple.OptionSet;

/**
 * Created by kaufmannkr on 12/23/15.
 */
public class MaxwellMysqlConfig {

	public String mysqlHost;
	public Integer mysqlPort;
	public String mysqlUser;
	public String mysqlPassword;

	public MaxwellMysqlConfig() {
		this.mysqlHost = null;
		this.mysqlPort = null;
		this.mysqlUser = null;
		this.mysqlPassword = null;
	}

	public MaxwellMysqlConfig(String host, Integer port, String user, String password) {
		this.mysqlHost = host;
		this.mysqlPort = port;
		this.mysqlUser = user;
		this.mysqlPassword = password;
	}

	public void parseOptions( String prefix, OptionSet options) {
		if ( options.has(prefix + "host"))
			this.mysqlHost = (String) options.valueOf(prefix + "host");
		if ( options.has(prefix + "password"))
			this.mysqlPassword = (String) options.valueOf(prefix + "password");
		if ( options.has(prefix + "user"))
			this.mysqlUser = (String) options.valueOf(prefix + "user");
		if ( options.has(prefix + "port"))
			this.mysqlPort = Integer.valueOf((String) options.valueOf(prefix + "port"));
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
