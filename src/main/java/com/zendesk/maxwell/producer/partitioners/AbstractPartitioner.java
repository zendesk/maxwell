package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;

import java.io.IOException;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public abstract class AbstractPartitioner {
	abstract protected String hashString(RowMap r);
	abstract protected int hashValue(String s);

	static protected String getDatabase(RowMap r) {
		return r.getDatabase();
	}

	static protected String getTable(RowMap r) {
		return r.getTable();
	}

	static protected String getPrimaryKey(RowMap r) {
		return r.pkAsConcatString();
	}

	public int kafkaPartition(RowMap r, int numPartitions) {
		return Math.abs(hashValue(hashString(r)) % numPartitions);
	}
}





