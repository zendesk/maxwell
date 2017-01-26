package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractMaxwellPartitioner {
	HashStringProvider provider;
	List<String> partitionColumns = new ArrayList<String>();

	String partitionKeyFallback;

	public AbstractMaxwellPartitioner(String partitionKey, String csvPartitionColumns, String partitionKeyFallback) {
		switch (partitionKey) {
			case "table": this.provider = new HashStringTable();
				break;
			case "primary_key": this.provider = new HashStringPrimaryKey();
				break;
			case "column": this.provider = new HashStringColumn();
				break;
			case "database":
			default:
				this.provider = new HashStringDatabase();
				break;
		}
		if ( csvPartitionColumns != null )
			this.partitionColumns = Arrays.asList(csvPartitionColumns.split("\\s*,\\s*"));

		this.partitionKeyFallback = partitionKeyFallback;
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

	public String getHashString(RowMap r) {
		return provider.getHashString(r, this.partitionColumns, this.partitionKeyFallback);
	}
}
