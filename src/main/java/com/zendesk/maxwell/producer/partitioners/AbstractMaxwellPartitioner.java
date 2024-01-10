package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractMaxwellPartitioner {
	List<String> partitionColumns = new ArrayList<String>();
	private final PartitionBy partitionBy, partitionByFallback;

	private PartitionBy partitionByForString(String key) {


		if ( key == null )
			return PartitionBy.DATABASE;

		switch(key) {
			case "table":
				return PartitionBy.TABLE;
			case "database":
				return PartitionBy.DATABASE;
			case "primary_key":
				return PartitionBy.PRIMARY_KEY;
			case "transaction_id":
				return PartitionBy.TRANSACTION_ID;
			case "thread_id":
				return PartitionBy.THREAD_ID;
			case "column":
				return PartitionBy.COLUMN;
			case "random":
				return PartitionBy.RANDOM;
			default:
				throw new RuntimeException("Unknown partitionBy string: " + key);
		}
	}

	public AbstractMaxwellPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
		this.partitionBy = partitionByForString(partitionKey);
		this.partitionByFallback = partitionByForString(partitionKeyFallback);

		if ( csvPartitionColumns != null )
			this.partitionColumns = Arrays.asList(csvPartitionColumns.split("\\s*,\\s*"));
	}

	static protected String getDatabase(RowMap r) {
		return r.getDatabase();
	}

	static protected String getTable(RowMap r) {
		return r.getTable();
	}

	public String getHashString(RowMap r, PartitionBy by) {
		switch ( by ) {
			case TABLE:
				String t = r.getTable();
				if ( t == null && partitionByFallback == PartitionBy.DATABASE )
					return r.getDatabase();
				else
					return t;
			case DATABASE:
				return r.getDatabase();
			case PRIMARY_KEY:
				return r.getRowIdentity().toConcatString();
			case TRANSACTION_ID:
				return String.valueOf(r.getXid());
			case THREAD_ID:
				return String.valueOf(r.getThreadId());
			case COLUMN:
				String s = r.buildPartitionKey(partitionColumns);
				if ( s.length() > 0 )
					return s;
				else
					return getHashString(r, partitionByFallback);
			case RANDOM:
				return RandomStringUtils.random(10, true, true);
		}
		return null; // thx java
	}

	public String getHashString(RowMap r) {
		if ( r.getPartitionString() != null )
			return r.getPartitionString();
		else
			return getHashString(r, partitionBy);
	}
}
