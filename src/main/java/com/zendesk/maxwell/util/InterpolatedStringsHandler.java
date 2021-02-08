package com.zendesk.maxwell.util;

import com.zendesk.maxwell.row.RowIdentity;
import com.zendesk.maxwell.row.RowMap;

/**
 * An utility class to interpolate subscription, channel templates.
 * <p>
 * An input string can contains any combination of:
 * - %{db}
 * - %{table}
 * - %{type}
 */
public class InterpolatedStringsHandler {
	private final String inputString;

	private final boolean isInterpolated;

	public InterpolatedStringsHandler(String inputString) {
		this.inputString = inputString;
		this.isInterpolated = inputString.contains("%{");
	}

	/**
	 * Interpolate the input string based on {{{@link RowIdentity}}}
	 * <p>
	 * If your inputString contains a %{type} it will not be interpolated because we can't get type from RowIdentity.
	 *
	 * @param pk the rowIdentity
	 * @return the interpollated string
	 */
	public String generateFromRowIdentity(RowIdentity pk) {
		String table = pk.getTable();
		if (table == null)
			table = "";

		if (this.isInterpolated)
			return interpolate(pk.getDatabase(), table, null);
		else
			return this.inputString;
	}

	/**
	 * Interpolate the input string based on {{{@link RowMap}}}
	 *
	 * @param r the rowMap
	 * @return the interpollated string
	 */
	public String generateFromRowMap(RowMap r) {
		String table = r.getTable();
		if (table == null)
			table = "";

		String type = r.getRowType();

		if (type == null)
			type = "";

		if (this.isInterpolated)
			return interpolate(r.getDatabase(), table, type);
		else
			return this.inputString;
	}

	protected String interpolate(final String database, final String table, final String type) {
		if (this.isInterpolated) {
			final String typeReplacement = type != null ? type : "";

			return this.inputString
					.replaceAll("%\\{database\\}", database)
					.replaceAll("%\\{table\\}", table)
					.replaceAll("%\\{type\\}", typeReplacement);
		} else {
			return this.inputString;
		}
	}

	public String generateFromRowMapAndTrimAllWhitesSpaces(RowMap r){
		return this.generateFromRowMap(r).replaceAll("\\s+","");
	}
}
