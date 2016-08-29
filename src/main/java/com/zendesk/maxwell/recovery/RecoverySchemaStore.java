package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ben on 8/29/16.
 */
public class RecoverySchemaStore implements SchemaStore {

	private ConnectionPool replicationConnectionPool;
	private String maxwellDatabaseName;
	private CaseSensitivity caseSensitivity;
	private Schema maxwellOnlySchema;

	public RecoverySchemaStore(ConnectionPool replicationConnectionPool,
							   String maxwellDatabaseName,
							   CaseSensitivity caseSensitivity) {

		this.replicationConnectionPool = replicationConnectionPool;
		this.maxwellDatabaseName = maxwellDatabaseName;
		this.caseSensitivity = caseSensitivity;
	}

	@Override
	public Schema getSchema() throws SchemaStoreException {
		if ( maxwellOnlySchema != null )
			return maxwellOnlySchema;

		try(Connection conn = replicationConnectionPool.getConnection() ) {
			SchemaCapturer capturer = new SchemaCapturer(conn, caseSensitivity, maxwellDatabaseName);
			maxwellOnlySchema = capturer.capture();
		} catch (SQLException e) {
			throw new SchemaStoreException(e);
		}

		return maxwellOnlySchema;
	}

	@Override
	public List<ResolvedSchemaChange> processSQL(String sql, String currentDatabase, BinlogPosition position) throws SchemaStoreException, InvalidSchemaError {
		return new ArrayList<>();
	}
}
