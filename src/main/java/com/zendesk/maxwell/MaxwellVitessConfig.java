package com.zendesk.maxwell;

public class MaxwellVitessConfig {
	public String vtgateHost;
	public int vtgatePort;

	public String user;
	public String password;

	public String keyspace;
	public String shard;

	public boolean usePlaintext;
	public String tlsCA;
	public String tlsCert;
	public String tlsKey;
	public String tlsServerName;

	public MaxwellVitessConfig() {
		this.vtgateHost = "localhost";
		this.vtgatePort = 15991;
		this.usePlaintext = true;

		this.tlsCA = null;
		this.tlsCert = null;
		this.tlsKey = null;
		this.tlsServerName = null;

		this.user = null;
		this.password = null;

		this.keyspace = null;
		this.shard = "";
	}
}
