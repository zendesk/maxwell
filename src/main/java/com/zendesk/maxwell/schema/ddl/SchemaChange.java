package com.zendesk.maxwell.schema.ddl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.zendesk.maxwell.filtering.Filter;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.Schema;

public abstract class SchemaChange {
    final static Logger LOGGER = LoggerFactory.getLogger(SchemaChange.class);
	public abstract ResolvedSchemaChange resolve(Schema schema) throws InvalidSchemaError;

	private static final Set<Pattern> SQL_BLACKLIST = new HashSet<Pattern>();

	private static final Pattern SET_STATEMENT = Pattern.compile("SET\\s+STATEMENT\\s+(\\w+\\s*=\\s*((?<quote>['\"]).*?\\k<quote>|\\w+),?\\s*)+FOR\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

	static {
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*BEGIN", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*COMMIT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*FLUSH", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*GRANT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*REVOKE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*SAVEPOINT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*CREATE\\s+(AGGREGATE)?\\s+FUNCTION", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(ALTER|CREATE)\\s+(DEFINER=[^\\s]+\\s+)?(EVENT|FUNCTION|TRIGGER|PROCEDURE)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*DROP\\s+(EVENT|FUNCTION|TRIGGER|PROCEDURE|VIEW)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(ALTER|CREATE|DROP)\\s+((ONLINE|OFFLINE|UNIQUE|FULLTEXT|SPATIAL)\\s+)*(INDEX)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*ANALYZE\\s+TABLE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*SET\\s+PASSWORD", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(ALTER|CREATE|DROP|RENAME)\\s+USER", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(ALTER|CREATE|DROP)\\s+TEMPORARY\\s+TABLE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(ALTER|CREATE|DROP)\\s+TABLESPACE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*(SET|DROP|CREATE)\\s+(DEFAULT\\s+)?ROLE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*TRUNCATE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*OPTIMIZE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("\\A\\s*REPAIR\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
	}

	private static final Pattern DELETE_BLACKLIST = Pattern.compile("^\\s*DELETE\\s*FROM", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

	private static final Pattern CSTYLE_COMMENTS = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);

	private static boolean matchesBlacklist(String sql) {
		// first *include* /*50032 CREATE EVENT */ style sql
		sql = sql.replaceAll("/\\*M?!\\d+\\s*(.*)\\*/", "$1");

		// now strip out comments
		sql = CSTYLE_COMMENTS.matcher(sql).replaceAll("");
		sql = sql.replaceAll("\\-\\-.*", "");
		sql = Pattern.compile("^\\s*#.*", Pattern.MULTILINE).matcher(sql).replaceAll("");

		// SET STATEMENT .. FOR syntax can be applied to BLACKLIST element, just omit for tesing purposes
		sql = SET_STATEMENT.matcher(sql).replaceAll("");

		for (Pattern p : SQL_BLACKLIST) {
			if (p.matcher(sql).find()) {
				LOGGER.debug("ignoring sql: {}", sql);
				return true;
			}
		}

		if ( DELETE_BLACKLIST.matcher(sql).find() ) {
			LOGGER.info("Ignoring DELETE statement: " + sql);
			LOGGER.info("You may ignore this warning if this is a MEMORY table.");
			LOGGER.info("Otherwise you should make sure your binlog_format setting is correct, and that your clients have all reconnected.");
			return true;
		}

		return false;
	}

	private static List<SchemaChange> parseSQL(String currentDB, String sql) {
		ANTLRInputStream input = new ANTLRInputStream(sql);
		mysqlLexer lexer = new mysqlLexer(input);
		lexer.removeErrorListeners();

		TokenStream tokens = new CommonTokenStream(lexer);

		LOGGER.debug("SQL_PARSE <- \"{}\"", sql);
		mysqlParser parser = new mysqlParser(tokens);
		parser.removeErrorListeners();

		MysqlParserListener listener = new MysqlParserListener(currentDB, tokens);

		ParseTree tree = parser.parse();

		ParseTreeWalker.DEFAULT.walk(listener, tree);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("SQL_PARSE ->   {}", tree.toStringTree(parser));
		}
		return listener.getSchemaChanges();
	}

	public static List<SchemaChange> parse(String currentDB, String sql) {
		if ( matchesBlacklist(sql) ) {
			return null;
		}

		while ( true ) {
			try {
				return parseSQL(currentDB, sql);
			} catch ( ReparseSQLException e ) {
				sql = e.getSQL();
				LOGGER.debug("rewrote SQL to {}", sql);
				// re-enter loop
			} catch ( ParseCancellationException e ) {
				if (LOGGER.isDebugEnabled()) {
					// we are debug logging the toString message, slf4j will log the stacktrace of a throwable
					String msg = e.toString();
					LOGGER.debug("Parse cancelled: {}", msg);
				}
				return null;
			} catch ( MaxwellSQLSyntaxError e) {
				LOGGER.error("Error parsing SQL: '{}'", sql);
				throw (e);
			}
		}
	}

	public abstract boolean isBlacklisted(Filter filter);
}
