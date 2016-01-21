package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashStringPrimaryKey implements HashStringProvider {
	public String getHashString(RowMap r) {
		return r.pkAsConcatString();
	}
}
