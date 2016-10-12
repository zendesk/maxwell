package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.row.RowMap;

import java.util.List;

/**
 * Created by smferguson on 10/5/16.
 */
public class HashStringColumn implements HashStringProvider {
    public String getHashString(RowMap r, List<String> partitionColumns, String partitionKeyFallback) {
        return r.buildPartitionKey(partitionColumns, partitionKeyFallback);
    }
}
