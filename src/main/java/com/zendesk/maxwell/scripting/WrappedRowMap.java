package com.zendesk.maxwell.scripting;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;

import java.util.LinkedHashMap;

// we pass this little wrapper into the javascript interface.
// this class is here so that we're not exposing the full RowMap,
// and so we can snake_case things properly.
public class WrappedRowMap {
	private final RowMap row;

	public WrappedRowMap(RowMap row) {
		this.row = row;
	}

	public LinkedHashMap<String, Object> getData() {
		return row.getData();
	}

	public LinkedHashMap<String, Object> getOld_data() {
		return row.getOldData();
	}

	public LinkedHashMap<String, Object> getExtra_attributes() {
		return row.getExtraAttributes();
	}

	public String getType() {
		return row.getRowType();
	}

	public String getTable() {
		return row.getTable();
	}

	public String getDatabase() {
		return row.getDatabase();
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

	public Long getXid() {
		return row.getXid();
	}

	public Long getServer_id() {
		return row.getServerId();
	}

	public Long getThread_id() {
		return row.getThreadId();
	}

	public Long getTimestamp() {
		return row.getTimestamp();
	}

	public void suppress() {
		row.suppress();
	}

	public void setKafka_topic(String topic) {
		row.setKafkaTopic(topic);
	}
}
