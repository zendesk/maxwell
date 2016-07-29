package com.zendesk.maxwell;

import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;

import joptsimple.OptionSet;

/**
 * Created by kaufmannkr on 12/23/15.
 */
public class MaxwellMysqlConfig {

	public String host;
	public Integer port;
	public String user;
	public String password;
	public ArrayList<String> jdbcOptions = new ArrayList<String>();

	public MaxwellMysqlConfig() {
		this.host = null;
		this.port = null;
		this.user = null;
		this.password = null;
	}

	public MaxwellMysqlConfig(String host, Integer port, String user, String password) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
	}

	public void setJDBCOptions(String opts) {
		if (opts == null)
			return;
		this.jdbcOptions = new ArrayList<>();
		this.jdbcOptions.add("zeroDateTimeBehavior=convertToNull");

		for ( String opt : opts.split("&") ) {
			this.jdbcOptions.add(opt.trim());
		}
	}

	public String getConnectionURI() {
		return "jdbc:mysql://" + host + ":" + port + "?" +
				StringUtils.join(this.jdbcOptions.toArray(), "&");
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() != this.getClass() ) return false;
		MaxwellMysqlConfig other = ((MaxwellMysqlConfig) obj);
		return (this.host == other.host
				&& this.password == other.password
				&& this.port == other.port
				&& this.user == other.user);
	}
}
