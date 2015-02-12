package com.zendesk.maxwell.schema.ddl;

import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;

public abstract class SchemaChange {
    final static Logger LOGGER = LoggerFactory.getLogger(SchemaChange.class);

	public abstract Schema apply(Schema originalSchema) throws SchemaSyncError;

	protected Database findDatabase(Schema schema, String dbName, boolean ignoreMissing) throws SchemaSyncError {
		Database database = schema.findDatabase(dbName);

		if ( database == null && !ignoreMissing )
			throw new SchemaSyncError("Couldn't find database " + dbName);

		return database;
	}

	protected Database findDatabase(Schema schema, String dbName) throws SchemaSyncError {
		return findDatabase(schema, dbName, false);
	}

	protected Table findTable(Database d, String tableName, boolean ignoreMissing) throws SchemaSyncError {
		Table table = d.findTable(tableName);

		if ( table == null && !ignoreMissing )
			throw new SchemaSyncError("Couldn't find table " + d.getName() + "." + tableName);
		return table;
	}

	protected Table findTable(Database d, String tableName) throws SchemaSyncError {
		return findTable(d, tableName, false);
	}

	protected Table findTable(Schema schema, String dbName, String tableName) throws SchemaSyncError {
		Database d = findDatabase(schema, dbName);
		return findTable(d, tableName);
	}


	public static List<SchemaChange> parse(String currentDB, String sql) {
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
	}

}
