package com.zendesk.maxwell.core.producer.partitioners;

/**
 * Created by kaufmannkr on 1/21/16.
 */
public interface HashFunction {
	int hashCode(String s);
}
