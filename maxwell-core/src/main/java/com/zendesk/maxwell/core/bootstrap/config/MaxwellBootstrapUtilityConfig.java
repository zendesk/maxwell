package com.zendesk.maxwell.core.bootstrap.config;

import com.zendesk.maxwell.core.config.MaxwellMysqlConfig;

public class MaxwellBootstrapUtilityConfig {
	public MaxwellMysqlConfig mysql;
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
		return "jdbc:mysql://" + mysql.host + ":" + mysql.port + "/" + schemaDatabaseName;
	}

	private void setDefaults() {
		if ( this.log_level == null ) {
			this.log_level = "WARN";
		}
	}
}
