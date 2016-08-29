package com.zendesk.maxwell.recovery;

import com.zendesk.maxwell.MaxwellFilter;

/**
 * filter out (via a blacklist) everything except for `maxwell`.`positions`
 */
public class RecoveryFilter extends MaxwellFilter {
	private final String maxwellDatabaseName;

	public RecoveryFilter(String maxwellDatabaseName) {
		this.maxwellDatabaseName = maxwellDatabaseName;
	}

	@Override
	public boolean isTableBlacklisted(String databaseName, String tableName) {
		return !(databaseName.equals(maxwellDatabaseName) && tableName.equals("positions"));
	}
}
