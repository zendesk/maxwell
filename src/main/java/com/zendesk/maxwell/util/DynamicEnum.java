package com.zendesk.maxwell.util;

import java.util.HashMap;

public class DynamicEnum {
	private long counter = 0;
	private final long maxLength;
	private final HashMap<Long, String> forwardMap = new HashMap<>();
	private final HashMap<String, Long> backwardsMap = new HashMap<>();

	public DynamicEnum(long maxLength) {
		this.maxLength = maxLength;
	}

	public String get(byte byID) {
		return get((long) byID);
	}

	public String get(short byID) {
		return get((long) byID);
	}

	public String get(int byID) {
		return get((long) byID);
	}

	public String get(long byID) {
		return forwardMap.get(byID);
	}

	public synchronized long get(String byString) {
		if ( byString == null )
			return -1;

		Long id = backwardsMap.get(byString);
		if ( id == null ) {
			forwardMap.put(counter, byString);
			backwardsMap.put(byString, counter);
			id = counter;
			if ( ++counter >= maxLength )
				throw new RuntimeException("Overflowed DynamicEnum!");
		}
		return id;
	}
}
