package com.zendesk.maxwell.scripting;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;

import java.util.LinkedHashMap;
import java.util.Map;

// we pass this little wrapper into the javascript interface.
// this class is here so that we're not exposing the full RowMap,
// and so we can snake_case things properly.
public class WrappedDDLMap {
	private final DDLMap row;

	public WrappedDDLMap(DDLMap row) {
		this.row = row;
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

	public String getSql() {
		return row.getSql();
	}

	public Map<String, Object> getChange() {
		return row.getChangeMap();
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

	public void suppress() {
		row.suppress();
	}

	public void setKafka_topic(String topic) {
		row.setKafkaTopic(topic);
	}
}
