package com.zendesk.exodus;

public class ExodusColumnInfo {
	public ExodusColumnInfo(String name, String encoding, Boolean unsigned) {
		this.name = name;
		this.encoding = encoding;
		this.unsigned = unsigned;
	}

	private String name;
	private String encoding;
	private Boolean unsigned;


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public Boolean getUnsigned() {
		return unsigned;
	}

	public void setUnsigned(Boolean unsigned) {
		this.unsigned = unsigned;
	}

}
