package com.zendesk.maxwell.core.recovery;

import com.zendesk.maxwell.core.config.BaseMaxwellFilter;

/**
 * filter out (via a blacklist) everything except for `maxwell`.`positions`.
 * this makes a possibly out of sync schema harmless.
 */
public class RecoveryFilter extends BaseMaxwellFilter {
	private final String maxwellDatabaseName;

	public RecoveryFilter(String maxwellDatabaseName) {
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	@Override
	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return !(databaseName.equals(maxwellDatabaseName) && tableName.equals("heartbeats"));
	}
}
