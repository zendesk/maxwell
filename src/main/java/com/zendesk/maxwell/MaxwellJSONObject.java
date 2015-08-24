package com.zendesk.maxwell;

import java.util.LinkedHashSet;
import java.util.Set;

import org.json.JSONObject;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent.RowMap;

public class MaxwellJSONObject extends JSONObject {
	public MaxwellJSONObject(RowMap map) {
		super(map);
	}

	// this preserves the key's output order.  vanity, I know, but nice.
	@Override
	public Set<String> keySet() {
		LinkedHashSet<String> set = new LinkedHashSet<>();

		set.add("database");
		set.add("table");
		set.add("type");
		set.add("data");
		set.add("ts");

		if ( has("xid") )
			set.add("xid");

		return set;
	}
}
