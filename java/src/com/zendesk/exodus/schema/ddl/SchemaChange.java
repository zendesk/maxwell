package com.zendesk.exodus.schema.ddl;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.zendesk.exodus.schema.Schema;

public abstract class SchemaChange {
	abstract Schema apply(Schema originalSchema) throws SchemaSyncError;

	static SchemaChange parse(String currentDB, String sql) {
		ANTLRInputStream input = new ANTLRInputStream(sql);
		mysqlLexer lexer = new mysqlLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		mysqlParser parser = new mysqlParser(tokens);

		ExodusMysqlParserListener listener = new ExodusMysqlParserListener(currentDB);

		System.out.println("Running parse on " + sql);
		ParseTree tree = parser.parse();

		ParseTreeWalker.DEFAULT.walk(listener, tree);
		System.out.println(tree.toStringTree(parser));

		return listener.getSchemaChange();
	}

}
