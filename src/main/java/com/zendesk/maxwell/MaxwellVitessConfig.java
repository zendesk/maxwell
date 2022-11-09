package com.zendesk.maxwell;

public class MaxwellVitessConfig {
  public String vtgateHost;
  public int vtgatePort;

  public String user;
  public String password;

  public String keyspace;
  public String shard;

  public MaxwellVitessConfig() {
    this.vtgateHost = "localhost";
    this.vtgatePort = 15991;

    this.user = null;
    this.password = null;

    this.keyspace = null;
    this.shard = "";
  }
}
