package com.zendesk.maxwell.scripting;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;

import java.util.LinkedHashMap;
import java.util.Map;

// we pass this little wrapper into the javascript interface.
// this class is here so that we're not exposing the full RowMap,
// and so we can snake_case things properly.
public class WrappedHeartbeatMap {
	private final HeartbeatRowMap row;

	public WrappedHeartbeatMap(HeartbeatRowMap row) {
		this.row = row;
	}

	public String getPosition() {
		BinlogPosition p = row.getPosition().getBinlogPosition();

		if ( p == null )
			return null;

		if ( p.getGtid() != null )
			return p.getGtid();
		else
			return p.getFile() + ":" + p.getOffset();
	}

	public Long getTimestamp() {
		return row.getTimestamp();
	}

	public Long getHeartbeat() {
		return row.getPosition().getLastHeartbeatRead();
	}
}
