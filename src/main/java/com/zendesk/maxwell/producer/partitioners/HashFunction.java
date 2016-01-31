package com.zendesk.maxwell.producer.partitioners;

/**
 * Created by kaufmannkr on 1/21/16.
 */
public interface HashFunction {
	int hashCode(String s);
}
