package com.zendesk.maxwell.core.row;

import com.zendesk.maxwell.api.config.MaxwellOutputConfig;
import com.zendesk.maxwell.api.replication.Position;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public interface RowMap {
	//Do we want to encrypt this part?
	String pkToJson(KeyFormat keyFormat) throws IOException;

	String pkAsConcatString();

	String buildPartitionKey(List<String> partitionColumns);

	String toJSON() throws Exception;

	String toJSON(MaxwellOutputConfig outputConfig) throws Exception;

	Object getData(String key);

	Object getExtraAttribute(String key);

	long getApproximateSize();

	void putData(String key, Object value);

	void putExtraAttribute(String key, Object value);

	Object getOldData(String key);

	void putOldData(String key, Object value);

	Position getPosition();

	Long getXid();

	void setXid(Long xid);

	Long getXoffset();

	void setXoffset(Long xoffset);

	void setTXCommit();

	boolean isTXCommit();

	Long getServerId();

	void setServerId(Long serverId);

	Long getThreadId();

	void setThreadId(Long threadId);

	String getDatabase();

	String getTable();

	Long getTimestamp();

	Long getTimestampMillis();

	boolean hasData(String name);

	String getRowQuery();

	String getRowType();

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	boolean shouldOutput(MaxwellOutputConfig outputConfig);

	LinkedHashMap<String, Object> getData();

	LinkedHashMap<String, Object> getExtraAttributes();

	LinkedHashMap<String, Object> getOldData();

	public enum KeyFormat { HASH, ARRAY }
}
