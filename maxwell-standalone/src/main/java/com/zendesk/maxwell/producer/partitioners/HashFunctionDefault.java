package com.zendesk.maxwell.producer.partitioners;

/**
 * Created by kaufmannkr on 1/18/16.
 */
public class HashFunctionDefault implements HashFunction {
	public int hashCode(String s) {
		return s.hashCode();
	}
}
