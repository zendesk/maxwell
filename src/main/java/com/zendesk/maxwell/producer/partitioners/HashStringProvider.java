package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.List;

/**
 * Created by kaufmannkr on 1/21/16.
 */
public interface HashStringProvider {
	String getHashString(RowMap r, List<String> partitionColumns, String partitionKeyFallback);
}
