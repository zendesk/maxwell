package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.List;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashStringTable implements HashStringProvider {
	public String getHashString(RowMap r, List<String> partitionColumns, String partitionKeyFallback) {
		String t = r.getTable();

		// support table-to-database fallback for DDL that has no table associated
		if ( t == null && partitionKeyFallback.equals("database") )
			return r.getDatabase();
		else
			return r.getTable();
	}
}
