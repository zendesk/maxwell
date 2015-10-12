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
	private static final int MAX_ROWS_PER_SECOND = 500;
	private final ConnectionPool connectionPool;

	public SchemaScavenger(ConnectionPool pool) {
		this.connectionPool = pool;
	}

	private List<Long> getDeletedSchemas() throws SQLException {
		ArrayList<Long> list = new ArrayList<>();
		try ( Connection connection = connectionPool.getConnection() ) {
			ResultSet rs = connection.createStatement().executeQuery("select id from `maxwell`.`schemas` where deleted = 1");

			while ( rs.next() ) {
				list.add(rs.getLong("id"));
			}
		}

		return list;
	}

	private long deleteRows(Long id, String tName, Long maxToDelete) throws SQLException {
		try ( Connection connection = connectionPool.getConnection() ) {
			long nDeleted = connection.createStatement().executeUpdate(
				"DELETE FROM maxwell." + tName +
					" WHERE schema_id = " + id +
					" LIMIT " + maxToDelete
			);
			if (nDeleted > 0) {
				LOGGER.debug("deleted " + nDeleted + " rows from maxwell." + tName + " schema: " + id);
			}
			return nDeleted;
		}
	}

	private void deleteSchema(Long id) throws SQLException {
		try ( Connection connection = connectionPool.getConnection() ) {
			connection.createStatement().execute("delete from `maxwell`.`schemas` where id = " + id);
		}
	}

	public void deleteBatch(long toDelete) throws SQLException {
		String[] tables = { "columns", "tables", "databases" };

		List<Long> schemaIds = getDeletedSchemas();

		for ( Long id : schemaIds) {
			boolean finishedDelete = false;

			while ( toDelete > 0 && !finishedDelete ) {
				for ( String tName : tables ) {
					long deleted = deleteRows(id, tName, toDelete);

					if (tName.equals("databases") && deleted < toDelete) {
						deleteSchema(id);
						finishedDelete = true;
					}
					toDelete -= deleted;
				}
			}
		}
	}

	@Override
	protected void work() throws Exception {
		deleteBatch(MAX_ROWS_PER_SECOND);
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
