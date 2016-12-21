package com.zendesk.maxwell.replication;

import java.util.concurrent.TimeoutException;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Schema;

/**
 * Created by ben on 10/23/16.
 */
public interface Replicator {
	void setFilter(MaxwellFilter filter);
	void startReplicator() throws Exception;
	RowMap getRow() throws Exception;
	Long getLastHeartbeatRead();
	Schema getSchema() throws SchemaStoreException;

	boolean runLoop() throws Exception;
	void stopLoop() throws TimeoutException;
}
