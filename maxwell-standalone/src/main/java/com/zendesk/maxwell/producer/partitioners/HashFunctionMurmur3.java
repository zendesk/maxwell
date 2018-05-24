package com.zendesk.maxwell.producer.partitioners;

import com.zendesk.maxwell.util.MurmurHash3;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashFunctionMurmur3 implements HashFunction {
	private int seed;
	public HashFunctionMurmur3(int seed){
		this.seed = seed;
	}
	public int hashCode(String s) {
		return MurmurHash3.murmurhash3_x86_32(s, 0, s.length(), seed);
	}
}
