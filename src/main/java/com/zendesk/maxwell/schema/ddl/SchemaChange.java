package com.zendesk.maxwell.schema.ddl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.Schema;

public abstract class SchemaChange {
    final static Logger LOGGER = LoggerFactory.getLogger(SchemaChange.class);

	public abstract Schema apply(Schema originalSchema) throws SchemaSyncError;

	private static final Set<Pattern> SQL_BLACKLIST = new HashSet<Pattern>();

	static {
		SQL_BLACKLIST.add(Pattern.compile("^BEGIN", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^COMMIT", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^FLUSH", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^GRANT", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^REVOKE\\s+", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^SAVEPOINT", Pattern.CASE_INSENSITIVE));

		SQL_BLACKLIST.add(Pattern.compile("^(ALTER|CREATE)\\s+(DEFINER=[^\\s]+\\s+)?(EVENT|FUNCTION|TRIGGER|PROCEDURE)", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^DROP\\s+(EVENT|FUNCTION|TRIGGER|PROCEDURE|VIEW)", Pattern.CASE_INSENSITIVE));

		SQL_BLACKLIST.add(Pattern.compile("^(ALTER|CREATE|DROP)\\s+((ONLINE|OFFLINE|UNIQUE|FULLTEXT|SPATIAL)\\s+)*(INDEX)", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^ANALYZE\\s+TABLE", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^SET\\s+PASSWORD", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^(CREATE|DROP|RENAME)\\s+USER", Pattern.CASE_INSENSITIVE));

		SQL_BLACKLIST.add(Pattern.compile("^CREATE\\s+TEMPORARY\\s+TABLE", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^TRUNCATE\\s+", Pattern.CASE_INSENSITIVE));
		SQL_BLACKLIST.add(Pattern.compile("^OPTIMIZE\\s+", Pattern.CASE_INSENSITIVE));

		SQL_BLACKLIST.add(Pattern.compile("^REPAIR\\s+", Pattern.CASE_INSENSITIVE));
	}

	private static boolean matchesBlacklist(String sql) {
		for ( Pattern p : SQL_BLACKLIST ) {
			if ( p.matcher(sql).find() )
				return true;
		}

		return false;
	}

	public static List<SchemaChange> parse(String currentDB, String sql) {
		if ( matchesBlacklist(sql) ) {
			return null;
		}

		try {
			ANTLRInputStream input = new ANTLRInputStream(sql);
			mysqlLexer lexer = new mysqlLexer(input);
			lexer.removeErrorListeners();

			CommonTokenStream tokens = new CommonTokenStream(lexer);
			mysqlParser parser = new mysqlParser(tokens);
			parser.removeErrorListeners();

			MysqlParserListener listener = new MysqlParserListener(currentDB);

			LOGGER.debug("SQL_PARSE <- \"" + sql + "\"");
			ParseTree tree = parser.parse();

			ParseTreeWalker.DEFAULT.walk(listener, tree);
			LOGGER.debug("SQL_PARSE ->   " + tree.toStringTree(parser));
			return listener.getSchemaChanges();
		} catch ( ParseCancellationException e ) {
			LOGGER.debug("Parse cancelled: " + e);
			return null;
		} catch ( MaxwellSQLSyntaxError e) {
			LOGGER.error("Error parsing SQL: '" + sql + "'");
			throw (e);
		}

	}

}
