package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class MaxwellKafkaPrimaryKeyPartitioner extends AbstractPartitioner {
	@Override
	protected String hashString(RowMap r) {
		return getPrimaryKey(r);
	}

	@Override
	protected int hashValue(String s) {
		return s.hashCode();
	}
}
