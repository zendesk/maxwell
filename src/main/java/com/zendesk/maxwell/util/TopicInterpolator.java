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
public class TopicInterpolator {
	private final String inputString;

	private final boolean isInterpolated;

	public TopicInterpolator(final String templateString) {
		this.inputString = templateString;
		this.isInterpolated = templateString.contains("%{");
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

		if (this.isInterpolated)
			return interpolate(pk.getDatabase(), table, null, false);
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

		String type = r.getRowType();

		if (this.isInterpolated)
			return interpolate(r.getDatabase(), table, type, false);
		else
			return this.inputString;
	}

	/**
	 * Interpolate the input string based on {{{@link RowMap}}} and clean up all non alpha-numeric characters
	 *
	 * @param r the rowMap
	 * @return the interpollated string
	 */
	public String generateFromRowMapAndCleanUpIllegalCharacters(RowMap r) {
		String table = r.getTable();
		String type = r.getRowType();

		if (this.isInterpolated)
			return interpolate(r.getDatabase(), table, type, true).replaceAll("\\s+", "");
		else
			return this.inputString;
	}

	protected String interpolate(final String database, final String table, final String type, final Boolean cleanUpIllegalCharacters) {
		if (this.isInterpolated) {
			final String typeReplacement = type != null ? type : "";

			if (cleanUpIllegalCharacters)
				return this.inputString
						.replaceAll("%\\{database\\}", cleanupIllegalCharacters(emptyStringOnNull(database)))
						.replaceAll("%\\{table\\}", cleanupIllegalCharacters(emptyStringOnNull(table)))
						.replaceAll("%\\{type\\}", cleanupIllegalCharacters(emptyStringOnNull(typeReplacement)));
			else
				return this.inputString
						.replaceAll("%\\{database\\}", emptyStringOnNull(database))
						.replaceAll("%\\{table\\}", emptyStringOnNull(table))
						.replaceAll("%\\{type\\}", emptyStringOnNull(typeReplacement));

		} else {
			return this.inputString;
		}
	}

	private String emptyStringOnNull(final String value) {
		if (value == null) {
			return "";
		} else {
			return value;
		}
	}

	private String cleanupIllegalCharacters(final String value) {
		return value.replaceAll("([^A-Za-z0-9])", "_");
	}
}
