package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.RunLoopProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchemaScavenger extends RunLoopProcess implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaStore.class);
	private static final long MAX_ROWS_PER_SECOND = 500;
	private final ConnectionPool connectionPool;
	private final String schemaDatabaseName;

	public SchemaScavenger(ConnectionPool pool, String dbName) {
		this.connectionPool = pool;
		this.schemaDatabaseName = dbName;
	}

	private Connection getConnection() throws SQLException {
		Connection conn = this.connectionPool.getConnection();
		conn.setCatalog(this.schemaDatabaseName);
		return conn;
	}

	private List<Long> getDeletedSchemas() throws SQLException {
		ArrayList<Long> list = new ArrayList<>();
		try ( Connection connection = getConnection() ) {
			ResultSet rs = connection.createStatement().executeQuery("select id from `schemas` where deleted = 1 LIMIT 100");

			while ( rs.next() ) {
				list.add(rs.getLong("id"));
			}
		}

		return list;
	}

	public void deleteSchema(Long id, Long maxRowsPerSecond) throws SQLException {
		String[] tables = { "columns", "tables", "databases" };

		try ( Connection connection = getConnection() ) {
			for ( String tName : tables ) {
				for (;;) {
					long nDeleted = connection.createStatement().executeUpdate(
						"DELETE FROM `" + tName +
							"` WHERE schema_id = " + id +
							" LIMIT " + maxRowsPerSecond
					);

					if (isStopRequested())
						return;

					if (nDeleted == 0)
						break;

					LOGGER.debug("deleted " + nDeleted + " rows from "+ this.schemaDatabaseName + "." + tName + " schema: " + id);
					try { Thread.sleep(1000); } catch (InterruptedException e) { }

					if (isStopRequested())
						return;
				}
			}

			connection.createStatement().execute("delete from `schemas` where id = " + id);
		}
	}

	public void deleteSchemas(Long maxRowsPerSecond) throws SQLException {
		for ( Long id : getDeletedSchemas() ) {
			deleteSchema(id, maxRowsPerSecond);
		}
	}

	@Override
	protected void work() throws Exception {
		deleteSchemas(MAX_ROWS_PER_SECOND);
		try { Thread.sleep(1000); } catch ( InterruptedException e ) {}
	}

	@Override
	public void run() {
		try {
			runLoop();
		} catch ( Exception e ) {
			LOGGER.error("SchemaScavenger thread aborting after exception: " + e);
		}
	}
}
