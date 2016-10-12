package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.List;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashStringDatabase implements HashStringProvider{
	public String getHashString(RowMap r, List<String> partitionColumns, String partitionKeyFallback) {
		return r.getDatabase();
	}
}
