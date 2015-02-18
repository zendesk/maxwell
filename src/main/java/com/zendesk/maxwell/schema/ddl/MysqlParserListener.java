package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ErrorNode;

import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.mysqlParser.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MaxwellSQLSyntaxError extends RuntimeException {
	private static final long serialVersionUID = 140545518818187219L;

	public MaxwellSQLSyntaxError(String message) {
		super(message);
	}
}


public class MysqlParserListener extends mysqlBaseListener {
    final Logger LOGGER = LoggerFactory.getLogger(MysqlParserListener.class);

	private String tableName;
	private final ArrayList<SchemaChange> schemaChanges;
	private final String currentDatabase;
	private ColumnPosition columnPosition;

	public List<SchemaChange> getSchemaChanges() {
		return schemaChanges;
	}

	private final LinkedList<ColumnDef> columnDefs = new LinkedList<>();

	MysqlParserListener(String currentDatabase)  {
		this.schemaChanges = new ArrayList<>();
		this.currentDatabase = currentDatabase;
	}

	private String unquote(String ident) {
		return ident.replaceAll("^`", "").replaceAll("`$", "");
	}

	private String unquote_literal(String ident) {
		return ident.replaceAll("^'", "").replaceAll("'$", "");
	}

	private String getDB(Table_nameContext t) {
		if ( t.db_name() != null )
			return unquote(t.db_name().name().getText());
		else
			return currentDatabase;
	}

	private String getTable(Table_nameContext t) {
		return unquote(t.name().getText());
	}

	private TableAlter alterStatement() {
		return (TableAlter)schemaChanges.get(0);
	}


	@Override
	public void visitErrorNode(ErrorNode node) {
		this.schemaChanges.clear();
		LOGGER.error(node.getParent().toStringTree(new mysqlParser(null)));
		throw new MaxwellSQLSyntaxError(node.getText());
	}

	private boolean isSigned(List<Int_flagsContext> flags) {
		for ( Int_flagsContext flag : flags ) {
			if ( flag.UNSIGNED() != null ) {
				return false;
			}
		}
		return true;
	}

	private ColumnPosition getColumnPosition() {
		// any time there's a possibility of a column position, we'll
		// want to clear it out so we don't re-use it next time.  visitors might be better in this case.
		ColumnPosition p = this.columnPosition;
		this.columnPosition = null;

		if ( p == null )
			return new ColumnPosition();
		else
			return p;
	}

	@Override
	public void exitAlter_table_preamble(Alter_table_preambleContext ctx) {
		String dbName = getDB(ctx.table_name());
		String tableName = getTable(ctx.table_name());

		TableAlter alterStatement = new TableAlter(dbName, tableName);
		this.tableName = alterStatement.tableName;

		this.schemaChanges.add(alterStatement);
	}

	@Override
	public void exitAdd_column(Add_columnContext ctx) {
		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement().columnMods.add(new AddColumnMod(c.getName(), c, getColumnPosition()));
	}
	@Override
	public void exitAdd_column_parens(mysqlParser.Add_column_parensContext ctx) {
		while ( this.columnDefs.size() > 0 ) {
			ColumnDef c = this.columnDefs.removeFirst();
			// unable to choose a position in this form
			alterStatement().columnMods.add(new AddColumnMod(c.getName(), c, null));
		}
	}
	@Override
	public void exitChange_column(mysqlParser.Change_columnContext ctx) {
		String oldColumnName = unquote(ctx.old_col_name().getText());

		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement().columnMods.add(new ChangeColumnMod(oldColumnName, c, getColumnPosition()));
	}
	@Override
	public void exitModify_column(mysqlParser.Modify_columnContext ctx) {
		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement().columnMods.add(new ChangeColumnMod(c.getName(), c, getColumnPosition()));
	}
	@Override
	public void exitDrop_column(mysqlParser.Drop_columnContext ctx) {
		alterStatement().columnMods.add(new RemoveColumnMod(unquote(ctx.old_col_name().getText())));
	}
	@Override
	public void exitCol_position(mysqlParser.Col_positionContext ctx) {
		this.columnPosition = new ColumnPosition();

		if ( ctx.FIRST() != null ) {
			this.columnPosition.position = ColumnPosition.Position.FIRST;
		} else if ( ctx.AFTER() != null ) {
			this.columnPosition.position = ColumnPosition.Position.AFTER;
			this.columnPosition.afterColumn = unquote(ctx.id().getText());
		}
	}
	@Override
	public void exitAlter_rename_table(Alter_rename_tableContext ctx) {
		alterStatement().newTableName = getTable(ctx.table_name());
		alterStatement().newDatabase  = getDB(ctx.table_name());
	}
	@Override
	public void exitConvert_to_character_set(mysqlParser.Convert_to_character_setContext ctx) {
		alterStatement().convertCharset = unquote_literal(ctx.charset_name().getText());
	}

	@Override
	public void exitDefault_character_set(mysqlParser.Default_character_setContext ctx) {
		// definitely hacky here; showing the fallacy of trying to mix and match listener
		// style parsing with more visitor-y stuff (in the exit nodes)
		if ( ctx.parent instanceof Alter_specificationContext )
			alterStatement().defaultCharset = unquote_literal(ctx.charset_name().getText());
	}

	@Override
	public void exitCreate_table_preamble(Create_table_preambleContext ctx) {
		String dbName = getDB(ctx.table_name());
		String tblName = getTable(ctx.table_name());

		TableCreate createStatement = new TableCreate(dbName, tblName);
		this.tableName = createStatement.tableName;

		this.schemaChanges.add(createStatement);
	}

	@Override
	public void exitCreate_specifications(Create_specificationsContext ctx) {
		TableCreate tableCreate = (TableCreate) schemaChanges.get(0);
		tableCreate.columns.addAll(this.columnDefs);
	}

	@Override
	public void exitCreation_character_set(Creation_character_setContext ctx) {
		TableCreate tableCreate = (TableCreate) schemaChanges.get(0);
		tableCreate.encoding = unquote_literal(ctx.charset_name().getText());
	}

	@Override
	public void exitDrop_table(mysqlParser.Drop_tableContext ctx) {
		boolean ifExists = ctx.if_exists() != null;
		for ( Table_nameContext t : ctx.table_name()) {
			schemaChanges.add(new TableDrop(getDB(t), getTable(t), ifExists));
		}
	}

	@Override
	public void exitDrop_database(mysqlParser.Drop_databaseContext ctx) {
		boolean ifExists = ctx.if_exists() != null;
		String dbName = unquote(ctx.name().getText());
		schemaChanges.add(new DatabaseDrop(dbName, ifExists));
	}


	@Override
	public void exitColumn_definition(mysqlParser.Column_definitionContext ctx) {
		String colType = null, colEncoding = null;
		boolean signed = true;

		String name = unquote(ctx.col_name.getText());

		Data_typeContext dctx = ctx.data_type();

		if ( dctx.generic_type() != null ) {
			colType = dctx.generic_type().col_type.getText();
		} else if ( dctx.signed_type() != null ) {
			colType = dctx.signed_type().col_type.getText();
			signed = isSigned(dctx.signed_type().int_flags());
		} else if ( dctx.string_type() != null ) {
			colType = dctx.string_type().col_type.getText();

			Charset_defContext charsetDef = dctx.string_type().charset_def();
			if ( charsetDef != null && charsetDef.character_set(0) != null ) {
				colEncoding = unquote_literal(charsetDef.character_set(0).charset_name().getText());
			}
		}

		ColumnDef c = ColumnDef.build(this.tableName,
					                   name,
					                   colEncoding,
					                   colType.toLowerCase(),
					                   -1,
					                   signed);
		this.columnDefs.add(c);
	}


	@Override
	public void exitRename_table_spec(Rename_table_specContext ctx) {
		Table_nameContext oldTableContext = ctx.table_name(0);
		Table_nameContext newTableContext = ctx.table_name(1);

		TableAlter t = new TableAlter(getDB(oldTableContext), getTable(oldTableContext));
		t.newDatabase  = getDB(newTableContext);
		t.newTableName = getTable(newTableContext);
		this.schemaChanges.add(t);
	}

	@Override
	public void exitCreate_database(mysqlParser.Create_databaseContext ctx) {
		String dbName = unquote(ctx.name().getText());
		boolean ifNotExists = ctx.if_not_exists() != null;
		String encoding = null;
		if ( ctx.default_character_set().size() > 0 ) {
			encoding = unquote_literal(ctx.default_character_set().get(0).charset_name().getText());
		}

		this.schemaChanges.add(new DatabaseCreate(dbName, ifNotExists, encoding));
	}
}