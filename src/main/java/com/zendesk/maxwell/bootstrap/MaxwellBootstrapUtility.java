package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.Logging;
import com.zendesk.maxwell.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MaxwellBootstrapUtility is a command line utility that launches and monitors the progress of bootstrapping.
 * The actual work of bootstrapping is done in the main maxwell server process.
 */
public class MaxwellBootstrapUtility {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtility.class);
	protected class MissingBootstrapRowException extends Exception {
		MissingBootstrapRowException(Long rowID) { super("Could not find bootstrap row with id: " + rowID); }
	}

	private static final long UPDATE_PERIOD_MILLIS = 250;
	private static final long DISPLAY_PROGRESS_WARMUP_MILLIS = 5000;
	private static final long NON_CONSOLE_DISPLAY_LINE_COUNT_MULTIPLE = 100000;

	private static Console console = System.console();

	private boolean isComplete = false;

	private void run(String[] argv) throws Exception {
		MaxwellBootstrapUtilityConfig config = new MaxwellBootstrapUtilityConfig(argv);

		if ( config.log_level != null ) {
			Logging.setLevel(config.log_level);
		}

		ConnectionPool connectionPool = getConnectionPool(config);
		ConnectionPool replConnectionPool = getReplicationConnectionPool(config);
		try ( final Connection connection = connectionPool.getConnection();
				final Connection replicationConnection = replConnectionPool.getConnection() ) {
			if ( config.abortBootstrapID != null ) {
				getInsertedRowsCount(connection, config.abortBootstrapID);
				removeBootstrapRow(connection, config.abortBootstrapID);
				return;
			}

			long rowId;
			if ( config.monitorBootstrapID != null ) {
				getInsertedRowsCount(connection, config.monitorBootstrapID);
				rowId = config.monitorBootstrapID;
			} else {
				Long totalRows = calculateRowCount(replicationConnection, config.databaseName, config.tableName, config.whereClause);
				rowId = insertBootstrapStartRow(connection, config.databaseName, config.tableName, config.whereClause, config.clientID, config.comment, totalRows);
			}

			try {
				monitorProgress(connection, rowId);
			} catch ( MissingBootstrapRowException e ) {
				LOGGER.error("bootstrap aborted.");
				Runtime.getRuntime().halt(1);
			}

		} catch ( SQLException e ) {
			LOGGER.error("failed to connect to mysql server @ " + config.getConnectionURI());
			LOGGER.error(e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}


	private void monitorProgress(Connection connection, Long rowId) throws SQLException, MissingBootstrapRowException {
		addMonitorShutdownHook(rowId);

		long rowCount = getTotalRowCount(connection, rowId);
		long initialRowCount, insertedRowsCount;
		initialRowCount = getInsertedRowsCount(connection, rowId);
		Long startedTimeMillis = null;

		insertedRowsCount = initialRowCount;
		while ( !isComplete ) {
			if ( insertedRowsCount < rowCount ) {
				if ( startedTimeMillis == null && insertedRowsCount > 0 ) {
					startedTimeMillis = System.currentTimeMillis();
				}
				insertedRowsCount = getInsertedRowsCount(connection, rowId);
			}
			isComplete = getIsComplete(connection, rowId);
			displayProgress(rowCount, insertedRowsCount, initialRowCount, startedTimeMillis);
			try {
				Thread.sleep(UPDATE_PERIOD_MILLIS);
			} catch ( InterruptedException e) {}
		}
		displayLine("");
	}

	private void addMonitorShutdownHook(final Long rowId) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if ( !isComplete && console != null ) {
					System.out.println("");
					System.out.println("Exiting monitor.  Bootstrap will continue in the background.");
					System.out.println("To abort, run maxwell-bootstrap --abort " + rowId);
					System.out.println("To resume monitoring, run maxwell-bootstrap --monitor " + rowId);
				}
			}
		});
	}

	private long getInsertedRowsCount(Connection connection, long rowId) throws SQLException, MissingBootstrapRowException {
		String sql = "select inserted_rows from `bootstrap` where id = ?";
		try ( PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {
			preparedStatement.setLong(1, rowId);
			try ( ResultSet resultSet = preparedStatement.executeQuery() ) {
				if ( resultSet.next() ) {
					return resultSet.getLong(1);
				} else {
					throw new MissingBootstrapRowException(rowId);
				}
			}
		}
	}

	private boolean getIsComplete(Connection connection, long rowId) throws SQLException, MissingBootstrapRowException {
		String sql = "select is_complete from `bootstrap` where id = ?";
		try ( PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {
			preparedStatement.setLong(1, rowId);
			try ( ResultSet resultSet = preparedStatement.executeQuery() ) {
				if ( resultSet.next() )  {
					return resultSet.getInt(1) == 1;
				} else {
					throw new MissingBootstrapRowException(rowId);
				}
			}
		}
	}

	private ConnectionPool getConnectionPool(MaxwellBootstrapUtilityConfig config) throws SQLException {
		String connectionURI = config.getConnectionURI();
		System.out.println("connecting to " + connectionURI);
		return new C3P0ConnectionPool(connectionURI, config.mysql.user, config.mysql.password);
	}

	private ConnectionPool getReplicationConnectionPool(MaxwellBootstrapUtilityConfig config) throws SQLException {
		String connectionURI = config.getReplicationConnectionURI();

		return new C3P0ConnectionPool(connectionURI, config.replicationMysql.user, config.replicationMysql.password);
	}

	private Long getTotalRowCount(Connection connection, Long bootstrapRowID) throws SQLException, MissingBootstrapRowException {
		try ( Statement stmt = connection.createStatement();
		      ResultSet resultSet = stmt.executeQuery("select total_rows from `bootstrap` where id = " + bootstrapRowID) ) {
			if ( resultSet.next() ) {
				return resultSet.getLong(1);
			} else {
				throw new MissingBootstrapRowException(bootstrapRowID);
			}
		}
	}

	private Long calculateRowCount(Connection connection, String db, String table, String whereClause) throws SQLException {
		LOGGER.info("counting rows");
		String sql = String.format("select count(*) from `%s`.`%s`", db, table);
		if ( whereClause != null ) {
			sql += String.format(" where %s", whereClause);
		}
		try ( PreparedStatement preparedStatement = connection.prepareStatement(sql);
			  ResultSet resultSet = preparedStatement.executeQuery() ) {
			resultSet.next();
			return resultSet.getLong(1);
		}
	}

	private long insertBootstrapStartRow(Connection connection, String db, String table, String whereClause, String clientID, String comment, Long totalRows) throws SQLException {
		LOGGER.info("inserting bootstrap start row");
		String sql = "insert into `bootstrap` (database_name, table_name, where_clause, total_rows, client_id, comment) values(?, ?, ?, ?, ?, ?)";

		try ( PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) ) {
			preparedStatement.setString(1, db);
			preparedStatement.setString(2, table);

			preparedStatement.setString(3, whereClause);
			preparedStatement.setLong(4, totalRows);
			preparedStatement.setString(5, clientID);
			preparedStatement.setString(6, comment);

			preparedStatement.execute();
			try ( ResultSet generatedKeys = preparedStatement.getGeneratedKeys() ) {
				generatedKeys.next();
				return generatedKeys.getLong(1);
			}
		}
	}

	private void removeBootstrapRow(Connection connection, long rowId) throws SQLException {
		LOGGER.info("deleting bootstrap start row");
		String sql = "delete from `bootstrap` where id = ?";
		try ( PreparedStatement preparedStatement = connection.prepareStatement(sql) ) {
			preparedStatement.setLong(1, rowId);
			preparedStatement.execute();
		}
	}

	private void displayProgress(long total, long count, long initialCount, Long startedTimeMillis) {
		if ( startedTimeMillis == null ) {
			displayLine("waiting for bootstrap to start... ");
		}
		else if ( count < total) {
			long currentTimeMillis = System.currentTimeMillis();
			long elapsedMillis = currentTimeMillis - startedTimeMillis;
			long predictedTotalMillis = ( long ) ((elapsedMillis / ( float ) (count - initialCount)) * (total - initialCount));
			long remainingMillis = predictedTotalMillis - elapsedMillis;
			String duration = prettyDuration(remainingMillis, elapsedMillis);
			displayLine(String.format("%d / %d (%.2f%%) %s", count, total, ( count * 100.0 ) / total, duration), count);
		} else {
			displayLine("waiting for bootstrap to stop... ");
		}
	}

	private String prettyDuration(long millis, long elapsedMillis) {
		if ( elapsedMillis < DISPLAY_PROGRESS_WARMUP_MILLIS ) {
			return "";
		}
		long d = (millis / (1000 * 60 * 60 * 24));
		long h = (millis / (1000 * 60 * 60)) % 24;
		long m = (millis / (1000 * 60)) % 60;
		long s = (millis / (1000)) % 60;
		if ( d > 0 ) {
			return String.format("- %d days %02dh %02dm %02ds remaining ", d, h, m, s);
		} else if ( h > 0 ) {
			return String.format("- %02dh %02dm %02ds remaining ", h, m, s);
		} else if ( m > 0 ) {
			return String.format("- %02dm %02ds remaining ", m, s);
		} else if ( s > 0 ) {
			return String.format("- %02ds remaining ", s);
		} else {
			return "";
		}
	}

	private void displayLine(String line, long count) {
		if ( console == null && count > 0 && count % NON_CONSOLE_DISPLAY_LINE_COUNT_MULTIPLE == 0 ) {
			System.out.println(line);
		} else {
			displayLine(line);
		}
	}

	private void displayLine(String line) {
		if ( console != null ) {
			String ansiClearLine = "\u001b[2K";
			String ansiMoveCursorToColumnZero = "\u001b[G";
			System.out.print(ansiClearLine + ansiMoveCursorToColumnZero + line);
			System.out.flush();
		}
	}

	public static void main(String[] args) {
		try {
			new MaxwellBootstrapUtility().run(args);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			LOGGER.info("done.");
		}
	}
}
