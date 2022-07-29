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
				"--replay_binlog=pom.xml",
				"--filter=blacklist:test.*"
		});
		config.validate();
		ReplayBinlogStore schemaStore = new ReplayBinlogStore(mockConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
		Schema schema = schemaStore.getSchema();
		Long schemaID = schemaStore.getSchemaID();
		Assert.assertNotNull(schema);
		Assert.assertNotNull(schema.findDatabase("test"));
		Assert.assertNotNull(schema.findDatabase("maxwell"));
		Assert.assertNotNull(schemaID);

		Position position = mock(Position.class);

		List<ResolvedSchemaChange> dropTable = schemaStore.processSQL("drop table position", "maxwell", position);
		Assert.assertFalse(dropTable.isEmpty());

		List<ResolvedSchemaChange> dropSchema = schemaStore.processSQL("drop schema maxwell", "maxwell", position);
		Assert.assertFalse(dropSchema.isEmpty());

		List<ResolvedSchemaChange> nullSchema = schemaStore.processSQL("drop table tmp_01", "test", position);
		Assert.assertTrue(nullSchema.isEmpty());
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
		when(resultSet.next()).thenReturn(true, true, false, true, false);
		when(resultSet.getString(Mockito.anyString())).thenReturn("maxwell", "utf8mb4", "test", "utf8mb4", "position", "utf8mb4", null);

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
