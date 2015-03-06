package com.zendesk.maxwell.schema.ddl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.Schema;

public abstract class SchemaChange {
    final static Logger LOGGER = LoggerFactory.getLogger(SchemaChange.class);

	public abstract Schema apply(Schema originalSchema) throws SchemaSyncError;

	private static final Set<String> SQL_BLACKLIST = new HashSet<String>();
	static {
		SQL_BLACKLIST.add("BEGIN");
		SQL_BLACKLIST.add("COMMIT");

	}

	public static List<SchemaChange> parse(String currentDB, String sql) {
		if ( SQL_BLACKLIST.contains(sql))
			return null;

		try {
			ANTLRInputStream input = new ANTLRInputStream(sql);
			mysqlLexer lexer = new mysqlLexer(input);
			CommonTokenStream tokens = new CommonTokenStream(lexer);
			mysqlParser parser = new mysqlParser(tokens);

			MysqlParserListener listener = new MysqlParserListener(currentDB);

			LOGGER.debug("SQL_PARSE <- \"" + sql + "\"");
			ParseTree tree = parser.parse();

			ParseTreeWalker.DEFAULT.walk(listener, tree);
			LOGGER.debug("SQL_PARSE ->   " + tree.toStringTree(parser));
			return listener.getSchemaChanges();
		} catch ( MaxwellSQLSyntaxError e) {
			LOGGER.error("Error parsing SQL: '" + sql + "'");
			throw (e);
		}

	}

}
