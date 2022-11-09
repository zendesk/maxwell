package com.zendesk.maxwell.schema;

import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.util.ConnectionPool;

public class VitessPositionStore extends MysqlPositionStore {
  public VitessPositionStore(ConnectionPool pool, Long serverID, String clientID, boolean gtidMode) {
    super(pool, serverID, clientID, gtidMode);
  }

  @Override
  public void set(Position p) {
    // TODO: implement this for storing vtgid values
  }

  @Override
  public long heartbeat() throws Exception {
    return System.currentTimeMillis();
  }
}
