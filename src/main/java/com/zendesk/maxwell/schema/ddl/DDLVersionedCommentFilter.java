package com.zendesk.maxwell.schema.ddl;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Removes explicitly configured MySQL version comments before DDL parsing.
 * Patterns are matched against the normalized content inside the comment, not
 * against the complete SQL statement.
 */
public final class DDLVersionedCommentFilter {
	private static final Pattern VERSIONED_COMMENT = Pattern.compile(
		"/\\*!([0-9]*)\\s*(.*?)\\*/",
		Pattern.DOTALL
	);

	private DDLVersionedCommentFilter() { }

	public static String filter(String sql, List<Pattern> ignoredContentPatterns) {
		if (sql == null || ignoredContentPatterns == null || ignoredContentPatterns.isEmpty())
			return sql;

		Matcher matcher = VERSIONED_COMMENT.matcher(sql);
		StringBuffer result = new StringBuffer();
		while (matcher.find()) {
			String content = matcher.group(2).trim().replaceAll("\\s+", " ");
			boolean ignored = false;
			for (Pattern pattern : ignoredContentPatterns) {
				if (pattern.matcher(content).matches()) {
					ignored = true;
					break;
				}
			}

			matcher.appendReplacement(
				result,
				ignored ? "" : Matcher.quoteReplacement(matcher.group())
			);
		}
		matcher.appendTail(result);
		return result.toString();
	}
}
