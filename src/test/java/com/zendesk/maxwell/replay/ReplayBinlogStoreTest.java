package com.zendesk.maxwell.replay;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Table;
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
	public void testBinlog() throws SchemaStoreException, SQLException {
		ReplayConfig config = new ReplayConfig(new String[]{"--host=localhost"});
		ReplayBinlogStore schemaStore = new ReplayBinlogStore(mockConnectionPool(), CaseSensitivity.CONVERT_ON_COMPARE, config);
		Schema schema = schemaStore.getSchema();
		Long schemaID = schemaStore.getSchemaID();
		Assert.assertNotNull(schema);
		Assert.assertNotNull(schemaID);

		schema.addDatabase(mockDatabase("maxwell", "test"));
		Position position = mock(Position.class);

		List<ResolvedSchemaChange> dropTable = schemaStore.processSQL("drop table test", "maxwell", position);
		Assert.assertFalse(dropTable.isEmpty());

		List<ResolvedSchemaChange> dropSchema = schemaStore.processSQL("drop schema maxwell", "maxwell", position);
		Assert.assertFalse(dropSchema.isEmpty());
	}


	private ConnectionPool mockConnectionPool() throws SQLException {
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		when(metaData.getDatabaseMajorVersion()).thenReturn(8);
		when(metaData.getDatabaseMinorVersion()).thenReturn(8);

		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(false);

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

	private Database mockDatabase(String database, String table) {
		Database db = new Database(database, "utf8");
		Table tb = new Table();
		tb.setTable(table);
		db.addTable(tb);
		return db;
	}
}
