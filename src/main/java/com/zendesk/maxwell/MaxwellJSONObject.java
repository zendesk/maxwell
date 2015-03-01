package com.zendesk.maxwell;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

public class MaxwellJSONObject extends JSONObject {
	public MaxwellJSONObject(Map<String, Object> map) {
		super(map);
	}

	@Override
	public Set keySet() {
		LinkedHashSet<String> set = new LinkedHashSet<>();

		set.add("table");
		set.add("type");
		set.add("data");

		return set;
	}
}
