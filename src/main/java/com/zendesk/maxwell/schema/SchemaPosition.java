package com.zendesk.maxwell.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.zendesk.maxwell.BinlogPosition;

public class SchemaPosition {
	public static BinlogPosition get(Connection c, Long serverID) throws SQLException {
		PreparedStatement s = c.prepareStatement("SELECT * from `maxwell`.`positions` where server_id = ?");
		s.setLong(1, serverID);

		ResultSet rs = s.executeQuery();
		if ( !rs.next() )
			return null;

		return new BinlogPosition(rs.getLong("binlog_position"), rs.getString("binlog_file"));
	}

	public static void set(Connection c, Long serverID, BinlogPosition p) throws SQLException {
		String sql = "INSERT INTO `maxwell`.`positions` set "
				+ "server_id = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ? "
				+ "ON DUPLICATE KEY UPDATE binlog_file=?, binlog_position=?";
		PreparedStatement s = c.prepareStatement(sql);

		s.setLong(1, serverID);
		s.setString(2, p.getFile());
		s.setLong(3, p.getOffset());
		s.setString(4, p.getFile());
		s.setLong(5, p.getOffset());

		s.execute();
	}

}
