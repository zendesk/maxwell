package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.BinlogPosition;

public class SchemaPosition implements Runnable {
	static final Logger LOGGER = LoggerFactory.getLogger(SchemaPosition.class);
	private final Connection connection;
	private final Long serverID;
	private BinlogPosition lastPosition;
	private final AtomicReference<BinlogPosition> position;
	private final AtomicBoolean run;
	private Thread thread;

	public SchemaPosition(Connection c, Long serverID) {
		this.connection = c;
		this.serverID = serverID;
		this.lastPosition = null;
		this.position = new AtomicReference<>();
		this.run = new AtomicBoolean(false);
	}

	public void start() {
		this.thread = new Thread(this, "Position Flush Thread");
		this.run.set(true);
		thread.start();
	}

	public void stop() {
		this.run.set(false);

		thread.interrupt();

		while ( thread.isAlive() ) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) { }
		}
	}

	@Override
	public void run() {
		while ( true && run.get() ) {
			BinlogPosition newPosition = position.get();

			if ( newPosition != null && !newPosition.equals(lastPosition) ) {
				try {
					store(newPosition);
					lastPosition = newPosition;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) { }
		}
	}


	private void store(BinlogPosition newPosition) throws SQLException {
		String sql = "INSERT INTO `maxwell`.`positions` set "
				+ "server_id = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ? "
				+ "ON DUPLICATE KEY UPDATE binlog_file=?, binlog_position=?";
		PreparedStatement s = this.connection.prepareStatement(sql);

		LOGGER.debug("Writing initial position: " + newPosition);
		s.setLong(1, serverID);
		s.setString(2, newPosition.getFile());
		s.setLong(3, newPosition.getOffset());
		s.setString(4, newPosition.getFile());
		s.setLong(5, newPosition.getOffset());

		s.execute();
	}

	public void set(BinlogPosition p) {
		position.set(p);
	}

	public BinlogPosition get() throws SQLException {
		PreparedStatement s = this.connection.prepareStatement("SELECT * from `maxwell`.`positions` where server_id = ?");
		s.setLong(1, serverID);

		ResultSet rs = s.executeQuery();
		if ( !rs.next() )
			return null;

		return new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file"));
	}
}