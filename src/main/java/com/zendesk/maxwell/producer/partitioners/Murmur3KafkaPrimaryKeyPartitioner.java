package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.util.MurmurHash3;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class Murmur3KafkaPrimaryKeyPartitioner extends AbstractPartitioner {
	private int seed;
	private int offset=0;

	public Murmur3KafkaPrimaryKeyPartitioner(int seed){
		this.seed = seed;
	}

	@Override
	protected String hashString(RowMap r) {
		return getPrimaryKey(r);
	}

	@Override
	protected int hashValue(String s) {
		return MurmurHash3.murmurhash3_x86_32(s, offset, s.length(), seed);
	}
}
