package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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
			case "column":
				return PartitionBy.COLUMN;
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

	static protected String getPrimaryKey(RowMap r) {
		return r.pkAsConcatString();
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
				return r.pkAsConcatString();
			case COLUMN:
				String s = r.buildPartitionKey(partitionColumns);
				if ( s.length() > 0 )
					return s;
				else
					return getHashString(r, partitionByFallback);
		}
		return null; // thx java
	}

	public String getHashString(RowMap r) {
		return getHashString(r, partitionBy);
	}
}
