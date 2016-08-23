package com.zendesk.maxwell.schema.ddl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.zendesk.maxwell.MaxwellFilter;
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

	static {
		SQL_BLACKLIST.add(Pattern.compile("^\\s*BEGIN", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*COMMIT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*FLUSH", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*GRANT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*REVOKE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*SAVEPOINT", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("^\\s*CREATE\\s+(AGGREGATE)?\\s+FUNCTION", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		// 20151102 Dave S. Added ALGORITHM to the blacklist
		SQL_BLACKLIST.add(Pattern.compile("^\\s*ALTER\\s+TABLE.*PARTITION", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*(ALTER|CREATE)\\s+(DEFINER=[^\\s]+\\s+)?(EVENT|FUNCTION|TRIGGER|PROCEDURE|ALGORITHM)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*DROP\\s+(EVENT|FUNCTION|TRIGGER|PROCEDURE|VIEW)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("^\\s*(ALTER|CREATE|DROP)\\s+((ONLINE|OFFLINE|UNIQUE|FULLTEXT|SPATIAL)\\s+)*(INDEX)", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*ANALYZE\\s+TABLE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*SET\\s+PASSWORD", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*(ALTER|CREATE|DROP|RENAME)\\s+USER", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("^\\s*CREATE\\s+TEMPORARY\\s+TABLE", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*TRUNCATE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
		SQL_BLACKLIST.add(Pattern.compile("^\\s*OPTIMIZE\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));

		SQL_BLACKLIST.add(Pattern.compile("^\\s*REPAIR\\s+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE));
	}

	private static boolean matchesBlacklist(String sql) {
		// first *include* /*50032 CREATE EVENT */ style sql
		sql = sql.replaceAll("/\\*!\\d+\\s*(.*)\\*/", "$1");

		// now strip out comments
		sql = sql.replaceAll("/\\*.*?\\*/", "");
		sql = sql.replaceAll("\\-\\-.*", "");

		for (Pattern p : SQL_BLACKLIST) {
			if (p.matcher(sql).find())
				return true;
		}

		return false;
	}

	private static List<SchemaChange> parseSQL(String currentDB, String sql) {


		// Dave Removed Partition Clause from create partitioned tables
		if (sql.replace("\n", " ").replace("\r", " ").toUpperCase().matches("^\\s*CREATE\\s+TABLE.*PARTITION.*") ) {

			int position = sql.toUpperCase().indexOf("PARTITION");

			sql = sql.substring(0, (position-1));

		}


		ANTLRInputStream input = new ANTLRInputStream(sql);
		mysqlLexer lexer = new mysqlLexer(input);
		lexer.removeErrorListeners();

		TokenStream tokens = new CommonTokenStream(lexer);

		LOGGER.debug("SQL_PARSE <- \"" + sql + "\"");
		mysqlParser parser = new mysqlParser(tokens);
		parser.removeErrorListeners();

		MysqlParserListener listener = new MysqlParserListener(currentDB, tokens);

		ParseTree tree = parser.parse();

		ParseTreeWalker.DEFAULT.walk(listener, tree);
		LOGGER.debug("SQL_PARSE ->   " + tree.toStringTree(parser));
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
				LOGGER.debug("rewrote SQL to " + sql);
				// re-enter loop
			} catch ( ParseCancellationException e ) {
				LOGGER.debug("Parse cancelled: " + e);
				return null;
			} catch ( MaxwellSQLSyntaxError e) {
				LOGGER.error("Error parsing SQL: '" + sql + "'");
				throw (e);
			}
		}
	}

	public abstract boolean isBlacklisted(MaxwellFilter filter);
}
