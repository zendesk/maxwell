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

		String type = r.getRowType();

		if (this.isInterpolated)
			return interpolate(r.getDatabase(), table, type);
		else
			return this.inputString;
	}

	protected String interpolate(final String database, final String table, final String type) {
		if (this.isInterpolated) {
			final String typeReplacement = type != null ? type : "";


			return this.inputString
					.replaceAll("%\\{database\\}", cleanupIllegalCharacters(cleanupIllegalCharacters(emptyStringOnNull(database))))
					.replaceAll("%\\{table\\}", cleanupIllegalCharacters(cleanupIllegalCharacters(emptyStringOnNull(table))))
					.replaceAll("%\\{type\\}", cleanupIllegalCharacters(cleanupIllegalCharacters(emptyStringOnNull(typeReplacement))));
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
		return value
				.replaceAll("([^A-Za-z0-9]+|[\\.\\s]+)", "_");

	}

	public String generateFromRowMapAndTrimAllWhitesSpaces(RowMap r) {
		return this.generateFromRowMap(r).replaceAll("\\s+", "");
	}
}
