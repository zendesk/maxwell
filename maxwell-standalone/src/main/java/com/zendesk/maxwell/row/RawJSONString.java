package com.zendesk.maxwell.row;

// we wrap up raw json here and use the type of this class to allow
// tunneling pre-serialized JSON data through a RowMap

public class RawJSONString {
	public final String json;

	public RawJSONString(String json) {
		this.json = json;
	}
}
