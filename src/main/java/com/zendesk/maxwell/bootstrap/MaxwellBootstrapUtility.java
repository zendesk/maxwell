package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.MaxwellLogging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import snaq.db.ConnectionPool;

import java.io.Console;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MaxwellBootstrapUtility {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellBootstrapUtility.class);

	private static final long UPDATE_PERIOD_MILLIS = 250;
	private static final long DISPLAY_PROGRESS_WARMUP_MILLIS = 5000;
	private static Console console = System.console();

	private boolean isComplete = false;

	private void run(String[] argv) throws Exception {
		MaxwellBootstrapUtilityConfig config = new MaxwellBootstrapUtilityConfig(argv);
		if ( config.log_level != null ) {
			MaxwellLogging.setLevel(config.log_level);
		}
		ConnectionPool connectionPool = getConnectionPool(config);
		try ( final Connection connection = connectionPool.getConnection() ) {
			int rowCount = getRowCount(connection, config);
			final long rowId = insertBootstrapStartRow(connection, config);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						if ( !isComplete ) {
							displayLine("");
							LOGGER.warn("bootstrapping cancelled");
							removeBootstrapRow(connection, rowId);
						}
					} catch ( Exception e ) {
						System.exit(1);
					}
				}
			});
			int insertedRowsCount = 0;
			Long startedTimeMillis = null;
			while ( !isComplete ) {
				if ( insertedRowsCount < rowCount ) {
					if ( startedTimeMillis == null && insertedRowsCount > 0 ) {
						startedTimeMillis = System.currentTimeMillis();
					}
					insertedRowsCount = getInsertedRowsCount(connection, rowId);
				}
				isComplete = getIsComplete(connection, rowId);
				displayProgress(rowCount, insertedRowsCount, startedTimeMillis);
				Thread.sleep(UPDATE_PERIOD_MILLIS);
			}
			displayLine("");
		} catch ( SQLException e ) {
			LOGGER.error("failed to connect to mysql server @ " + config.getConnectionURI());
			LOGGER.error(e.getLocalizedMessage());
			System.exit(1);
		}
	}

	private int getInsertedRowsCount(Connection connection, long rowId) throws SQLException {
		String sql = "select inserted_rows from `bootstrap` where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, rowId);
		ResultSet resultSet = preparedStatement.executeQuery();
		resultSet.next();
		return resultSet.getInt(1);
	}

	private boolean getIsComplete(Connection connection, long rowId) throws SQLException {
		String sql = "select is_complete from `bootstrap` where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, rowId);
		ResultSet resultSet = preparedStatement.executeQuery();
		resultSet.next();
		return resultSet.getInt(1) == 1;
	}

	private ConnectionPool getConnectionPool(MaxwellBootstrapUtilityConfig config) {
		String name = "MaxwellBootstrapConnectionPool";
		int maxPool = 10;
		int maxSize = 0;
		int idleTimeout = 10;
		String connectionURI = config.getConnectionURI();
		String mysqlUser = config.mysqlUser;
		String mysqlPassword = config.mysqlPassword;
		return new ConnectionPool(name, maxPool, maxSize, idleTimeout, connectionURI, mysqlUser, mysqlPassword);
	}

	private int getRowCount(Connection connection, MaxwellBootstrapUtilityConfig config) throws SQLException {
		LOGGER.info("counting rows");
		String sql = String.format("select count(*) from %s.%s", config.databaseName, config.tableName);
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();
		resultSet.next();
		return resultSet.getInt(1);
	}

	private long insertBootstrapStartRow(Connection connection, MaxwellBootstrapUtilityConfig config) throws SQLException {
		LOGGER.info("inserting bootstrap start row");
		String sql = "insert into `bootstrap` (database_name, table_name) values(?, ?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, config.databaseName);
		preparedStatement.setString(2, config.tableName);
		preparedStatement.execute();
		ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
		generatedKeys.next();
		return generatedKeys.getLong(1);
	}

	private void removeBootstrapRow(Connection connection, long rowId) throws SQLException {
		LOGGER.info("deleting bootstrap start row");
		String sql = "delete from `bootstrap` where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setLong(1, rowId);
		preparedStatement.execute();
	}

	private void displayProgress(int total, int count, Long startedTimeMillis) {
		if ( startedTimeMillis == null ) {
			displayLine("waiting for bootstrap to start... ");
		}
		else if ( count < total) {
			long currentTimeMillis = System.currentTimeMillis();
			long elapsedMillis = currentTimeMillis - startedTimeMillis;
			long predictedTotalMillis = ( long ) ((elapsedMillis / ( float ) count) * total);
			long remainingMillis = predictedTotalMillis - elapsedMillis;
			String duration = prettyDuration(remainingMillis, elapsedMillis);
			displayLine(String.format("%d / %d (%.2f%%) %s", count, total, ( count * 100.0 ) / total, duration));
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
