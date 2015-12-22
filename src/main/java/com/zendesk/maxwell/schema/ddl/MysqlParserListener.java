package com.zendesk.maxwell.schema.ddl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;

import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.mysqlParser.Column_optionsContext;
import com.zendesk.maxwell.schema.ddl.mysqlParser.Enum_valueContext;
import com.zendesk.maxwell.schema.ddl.mysqlParser.Index_columnContext;
import com.zendesk.maxwell.schema.ddl.mysqlParser.NameContext;
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

	private ArrayList<String> pkColumns;

	MysqlParserListener(String currentDatabase)  {
		this.pkColumns = null; // null indicates no change in primary keys
		this.schemaChanges = new ArrayList<>();
		this.currentDatabase = currentDatabase;
	}

	private String unquote(String ident) {
		return ident.replaceFirst("^`", "").replaceFirst("`$", "");
	}

	private String unquote_literal(String ident) {
		return unquote(ident.replaceAll("^'", "").replaceAll("'$", ""));
	}

	private String getDB(Table_nameContext t) {
		if ( t.db_name() != null )
			return unquote(t.db_name().getText());
		else
			return currentDatabase;
	}

	private String getTable(Table_nameContext t) {
		return unquote(t.name().getText());
	}

	private TableAlter alterStatement() {
		return (TableAlter)schemaChanges.get(0);
	}

	private String getEncoding(Charset_defContext ctx) {
		if ( ctx != null && ctx.character_set(0) != null ) {
			return unquote_literal(ctx.character_set(0).charset_name().getText());
		} else {
			return null;
		}
	}

	@Override
	public void visitErrorNode(ErrorNode node) {
		this.schemaChanges.clear();

		String error = node.getParent().toStringTree(new mysqlParser(null));
		LOGGER.error(error);
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
	public void exitAlter_database(Alter_databaseContext ctx) {
		String dbName = unquote(ctx.name().getText());
		DatabaseAlter alter = new DatabaseAlter(dbName);



		List<Default_character_setContext> charSet = ctx.alter_database_definition().default_character_set();
		if ( charSet.size() > 0 ) {
			alter.characterSet = unquote_literal(charSet.get(0).getText());
		}

		this.schemaChanges.add(alter);
	}

	@Override
	public void exitAlter_table_preamble(Alter_table_preambleContext ctx) {
		String dbName = getDB(ctx.table_name());
		String tableName = getTable(ctx.table_name());

		TableAlter alterStatement = new TableAlter(dbName, tableName);
		this.tableName = alterStatement.tableName;

		this.schemaChanges.add(alterStatement);
	}

	// After we're done parsing the whole alter
	@Override public void exitAlter_table(mysqlParser.Alter_tableContext ctx) {
		alterStatement().pks = this.pkColumns;
	}

	@Override
	public void enterAlter_view(Alter_viewContext ctx) {
		throw new ParseCancellationException("Not finishing parse of ALTER VIEW");
	}

	@Override
	public void enterCreate_view(Create_viewContext ctx) {
		throw new ParseCancellationException("Not finishing parse of CREATE VIEW");
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
		boolean ifNotExists = ctx.if_not_exists() != null;

		TableCreate createStatement = new TableCreate(dbName, tblName, ifNotExists);
		this.tableName = createStatement.tableName;

		this.schemaChanges.add(createStatement);
	}

	@Override
	public void exitCreate_like_tbl(Create_like_tblContext ctx) {
		TableCreate tableCreate = (TableCreate) schemaChanges.get(0);
		tableCreate.likeDB    = getDB(ctx.table_name());
		tableCreate.likeTable = getTable(ctx.table_name());
	}

	@Override
	public void exitCreate_specifications(Create_specificationsContext ctx) {
		TableCreate tableCreate = (TableCreate) schemaChanges.get(0);
		tableCreate.columns.addAll(this.columnDefs);
		tableCreate.pks = this.pkColumns;
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
	public void exitIndex_type_pk(mysqlParser.Index_type_pkContext ctx) {
		this.pkColumns = new ArrayList<>();
		for (  Index_columnContext column : ctx.index_column_list().index_columns().index_column() ) {
			NameContext n = column.name();
			this.pkColumns.add(unquote(n.getText()));
		}
	}

	@Override public void exitDrop_primary_key(mysqlParser.Drop_primary_keyContext ctx) {
		this.pkColumns = new ArrayList<>();
	}

	@Override
	public void exitColumn_definition(mysqlParser.Column_definitionContext ctx) {
		Boolean longStringFlag = false;
		String colType = null, colEncoding = null;
		String[] enumValues = null;
		List<Column_optionsContext> colOptions = null;
		boolean signed = true;

		String name = unquote(ctx.col_name.getText());

		Data_typeContext dctx = ctx.data_type();

		if ( dctx.generic_type() != null ) {
			colType = dctx.generic_type().col_type.getText();
			colOptions = dctx.generic_type().column_options();
		} else if ( dctx.signed_type() != null ) {
			colType = dctx.signed_type().col_type.getText();
			signed = isSigned(dctx.signed_type().int_flags());
			colOptions = dctx.signed_type().column_options();
		} else if ( dctx.string_type() != null ) {
			colType = dctx.string_type().col_type.getText();
			colEncoding = getEncoding(dctx.string_type().charset_def(0));
			colOptions = dctx.string_type().column_options();
			longStringFlag = (dctx.string_type().long_flag() != null);
		} else if ( dctx.enumerated_type() != null ) {
			List<Enum_valueContext> valueList = dctx.enumerated_type().enumerated_values().enum_value();

			colType = dctx.enumerated_type().col_type.getText();
			colEncoding = getEncoding(dctx.enumerated_type().charset_def(0));
			colOptions = dctx.enumerated_type().column_options();
			enumValues = new String[valueList.size()];

			int i = 0;
			for ( Enum_valueContext v : valueList ) {
				enumValues[i++] = unquote_literal(v.getText());
			}
		}

		colType = ColumnDef.unalias_type(colType.toLowerCase(), longStringFlag);
		ColumnDef c = ColumnDef.build(this.tableName,
					                   name,
					                   colEncoding,
					                   colType.toLowerCase(),
					                   -1,
					                   signed,
					                   enumValues);

		this.columnDefs.add(c);

		if ( colOptions != null ) {
			for (Column_optionsContext opt : colOptions) {
				if (opt.primary_key() != null) {
					this.pkColumns = new ArrayList<>();
					this.pkColumns.add(name);
				}
			}
		}
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