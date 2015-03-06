package com.zendesk.maxwell;

import java.util.LinkedHashSet;
import java.util.Set;

import org.json.JSONObject;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent.RowMap;

public class MaxwellJSONObject extends JSONObject {
	public MaxwellJSONObject(RowMap map) {
		super(map);
	}

	@Override
	public Set<String> keySet() {
		LinkedHashSet<String> set = new LinkedHashSet<>();

		set.add("table");
		set.add("type");
		set.add("data");

		return set;
	}
}
