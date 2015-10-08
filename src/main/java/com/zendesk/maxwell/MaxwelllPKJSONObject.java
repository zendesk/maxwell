package com.zendesk.maxwell;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

public class MaxwelllPKJSONObject extends JSONObject {
	private final ArrayList<String> pkKeys;

	public MaxwelllPKJSONObject(String db, String table, Map<String, Object> pks) {
		super();

		this.put("database", db);
		this.put("table", table);
		this.pkKeys = new ArrayList<String>();

		ArrayList<String> sortedPKs = new ArrayList<String>(pks.keySet());
		Collections.sort(sortedPKs);

		for ( String pk : sortedPKs ) {
			String prefixed = "pk." + pk;
			this.pkKeys.add(prefixed);
			this.put(prefixed, pks.get(pk));
		}
	}

	@Override
	public Set<String> keySet() {
		LinkedHashSet<String> set = new LinkedHashSet<>();

		set.add("database");
		set.add("table");
		for ( String key : this.pkKeys ) {
			set.add(key);
		}

		return set;
	}

}
