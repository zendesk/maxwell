package com.zendesk.maxwell.core.bootstrap.config;

import com.zendesk.maxwell.core.config.BaseMaxwellMysqlConfig;

public class MaxwellBootstrapUtilityConfig {
	public BaseMaxwellMysqlConfig mysql;
	public String  databaseName;
	public String  schemaDatabaseName;
	public String  tableName;
	public String  whereClause;
	public String  log_level;
	public String  clientID;

	public Long    abortBootstrapID;
	public Long    monitorBootstrapID;

	public MaxwellBootstrapUtilityConfig() {
		this.setDefaults();
	}

	public String getConnectionURI( ) {
		return "jdbc:mysql://" + mysql.getHost() + ":" + mysql.getPort() + "/" + schemaDatabaseName;
	}

	private void setDefaults() {
		if ( this.log_level == null ) {
			this.log_level = "WARN";
		}
	}
}
