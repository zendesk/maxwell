package com.zendesk.maxwell.core.config;

public class BaseMaxwellDiagnosticConfig implements MaxwellDiagnosticConfig {
	private boolean enable;
	private long timeout;

	@Override
	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	@Override
	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
}
