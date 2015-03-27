package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaStoreMigrations {
	private final Connection connection;
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);

	public SchemaStoreMigrations(Connection connection) {
		this.connection = connection;
	}

	public void upgrade() throws SQLException {
		upgradeColumnsTable();
	}

	static final String pkTestSQL =
			"select * from information_schema.columns where table_catalog = 'maxwell' "
			+ " and table_name='columns' and column_name='pks'";

	static final String pkAlterSQL =
			"alter table maxwell.columns add column pk varchar(1024) NULL default NULL";

	private void upgradeColumnsTable() throws SQLException {
		ResultSet rs = connection.createStatement().executeQuery(pkTestSQL);
		if ( rs.next() ) {
			return;
		} else {
			LOGGER.info("upgrading maxwell.columns to include PK column");
			connection.createStatement().executeQuery(pkAlterSQL);
		}
	}
}
