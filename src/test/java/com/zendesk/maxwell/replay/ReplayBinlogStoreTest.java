package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.ConnectionPool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.*;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author udyr@shlaji.com
 */
public class ReplayBinlogStoreTest {

	@Test
	public void testProcessSQL() throws SchemaStoreException, SQLException {
		ReplayConfig config = new ReplayConfig(new String[]{
				"--host=127.0.0.1",
				"--replay_binlog=/data/binlog/binlog.000001",
				"--filter=blacklist:test.*,blacklist:bac_schema.tmp_01,exclude:bac_schema.bac_history"
		});
		config.validate();
		ReplayBinlogStore schemaStore = new ReplayBinlogStore(mockConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
		Schema schema = schemaStore.getSchema();
		Long schemaID = schemaStore.getSchemaID();
		Assert.assertNotNull(schema);
		Assert.assertNotNull(schema.findDatabase("maxwell"));
		Assert.assertNotNull(schema.findDatabase("test"));
		Assert.assertNotNull(schema.findDatabase("bac_schema"));
		Assert.assertNotNull(schemaID);

		Position position = mock(Position.class);

		List<ResolvedSchemaChange> dropTable = schemaStore.processSQL("drop table position", "maxwell", position);
		Assert.assertFalse(dropTable.isEmpty());

		dropTable = schemaStore.processSQL("drop table position", "maxwell", position);
		Assert.assertTrue(dropTable.isEmpty());

		List<ResolvedSchemaChange> dropSchema = schemaStore.processSQL("drop schema maxwell", "maxwell", position);
		Assert.assertFalse(dropSchema.isEmpty());

		dropSchema = schemaStore.processSQL("drop schema maxwell", "maxwell", position);
		Assert.assertTrue(dropSchema.isEmpty());

		// Test database blocklist
		List<ResolvedSchemaChange> blackSchema = schemaStore.processSQL("drop schema test", "test", position);
		Assert.assertTrue(blackSchema.isEmpty());

		// Test table blocklist
		blackSchema = schemaStore.processSQL("drop table tmp_01", "bac_schema", position);
		Assert.assertTrue(blackSchema.isEmpty());

		// Test table rename
		List<ResolvedSchemaChange> renameTableSchema = schemaStore.processSQL("alter table history rename bac_history", "bac_schema", position);
		Assert.assertTrue(renameTableSchema.isEmpty());
	}


	/**
	 * mock data
	 *
	 * @return maxwell :> position
	 * @throws SQLException Connection SQLException
	 */
	private ConnectionPool mockConnectionPool() throws SQLException {
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		when(metaData.getDatabaseMajorVersion()).thenReturn(8);
		when(metaData.getDatabaseMinorVersion()).thenReturn(8);

		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true, true, true, false, true, false);
		when(resultSet.getString(Mockito.anyString())).thenReturn("maxwell", "utf8mb4", "test", "utf8mb4", "bac_schema", "utf8mb4",
				"position", "utf8mb4", "history", "utf8mb4", null);

		PreparedStatement statement = mock(PreparedStatement.class);
		when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
		when(statement.executeQuery()).thenReturn(resultSet);

		Connection connection = mock(Connection.class);
		when(connection.getMetaData()).thenReturn(metaData);
		when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
		when(connection.createStatement()).thenReturn(statement);

		ConnectionPool connectionPool = mock(ConnectionPool.class);
		when(connectionPool.getConnection()).thenReturn(connection);
		return connectionPool;
	}
}
