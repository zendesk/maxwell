package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;

import java.util.List;

/**
 * Created by smferguson on 10/5/16.
 */
public class HashStringColumn implements HashStringProvider {
    public String getHashString(RowMap r) {
        throw new UnsupportedOperationException("getHashString(RowMap) not implemented for HashStringColumn");
    }

    public String getHashString(RowMap r, List<String> partitionColumns) {
        return r.buildPartitionKey(partitionColumns);
    }
}