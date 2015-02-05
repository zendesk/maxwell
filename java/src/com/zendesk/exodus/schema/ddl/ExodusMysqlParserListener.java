package com.zendesk.exodus.schema.ddl;

import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.tree.ErrorNode;

import com.zendesk.exodus.schema.columndef.ColumnDef;
import com.zendesk.exodus.schema.ddl.mysqlParser.*;

abstract class ColumnMod {
	public String name;

	public ColumnMod(String name) {
		this.name = name;
	}
}

class ColumnPosition {
	enum Position { FIRST, AFTER, DEFAULT };

	public Position position;
	public String afterColumn;

	public ColumnPosition() {
		position = Position.DEFAULT;
	}
}

class AddColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public AddColumnMod(String name, ColumnDef d, ColumnPosition position) {
		super(name);
		this.definition = d;
		this.position = position;
	}
}

class ChangeColumnMod extends ColumnMod {
	public ColumnDef definition;
	public ColumnPosition position;

	public ChangeColumnMod(String name, ColumnDef d, ColumnPosition position ) {
		super(name);
		this.definition = d;
		this.position = position;
	}
}

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}
}

class ExodusSQLSyntaxRrror extends RuntimeException {
	private static final long serialVersionUID = 140545518818187219L;

	public ExodusSQLSyntaxRrror(String message) {
		super(message);
	}
}

public class ExodusMysqlParserListener extends mysqlBaseListener {
	private String tableName;
	private TableAlter alterStatement;
	private TableCreate createStatement;
	private final String currentDatabase;
	private ColumnPosition columnPosition;


	private final LinkedList<ColumnDef> columnDefs = new LinkedList<>();

	public TableAlter getAlterStatement() {
		return alterStatement;
	}

	public TableCreate getCreateStatement() {
		return createStatement;
	}

	ExodusMysqlParserListener(String currentDatabase)  {
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
			return unquote(t.db_name().id().getText());
		else
			return null;
	}

	private String getTable(Table_nameContext t) {
		return unquote(t.id().getText());
	}


	@Override
	public void visitErrorNode(ErrorNode node) {
		this.alterStatement = null;
		System.out.println(node.getParent().toStringTree(new mysqlParser(null)));
		throw new ExodusSQLSyntaxRrror(node.getText());
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
		return p;
	}

	@Override
	public void exitAlter_tbl_preamble(Alter_tbl_preambleContext ctx) {

		alterStatement = new TableAlter(currentDatabase);

		alterStatement.database  = getDB(ctx.table_name());
		this.tableName = alterStatement.tableName = getTable(ctx.table_name());

		System.out.println(alterStatement);
	}

	@Override
	public void exitAdd_column(Add_columnContext ctx) {
		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement.columnMods.add(new AddColumnMod(c.getName(), c, getColumnPosition()));
	}
	@Override
	public void exitAdd_column_parens(mysqlParser.Add_column_parensContext ctx) {
		while ( this.columnDefs.size() > 0 ) {
			ColumnDef c = this.columnDefs.removeFirst();
			// unable to choose a position in this form
			alterStatement.columnMods.add(new AddColumnMod(c.getName(), c, null));
		}
	}
	@Override
	public void exitChange_column(mysqlParser.Change_columnContext ctx) {
		String oldColumnName = unquote(ctx.old_col_name().getText());

		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement.columnMods.add(new ChangeColumnMod(oldColumnName, c, getColumnPosition()));
	}
	@Override
	public void exitModify_column(mysqlParser.Modify_columnContext ctx) {
		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement.columnMods.add(new ChangeColumnMod(c.getName(), c, getColumnPosition()));
	}
	@Override
	public void exitDrop_column(mysqlParser.Drop_columnContext ctx) {
		alterStatement.columnMods.add(new RemoveColumnMod(unquote(ctx.old_col_name().getText())));
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
	public void exitRename_table(mysqlParser.Rename_tableContext ctx) {
		alterStatement.newTableName = getTable(ctx.table_name());
		alterStatement.newDatabase  = getDB(ctx.table_name());
	}
	@Override
	public void exitConvert_to_character_set(mysqlParser.Convert_to_character_setContext ctx) {
		alterStatement.convertCharset = unquote_literal(ctx.charset_name().getText());
	}
	@Override
	public void exitDefault_character_set(mysqlParser.Default_character_setContext ctx) {
		alterStatement.defaultCharset = unquote_literal(ctx.charset_name().getText());
	}

	@Override
	public void exitCreate_tbl_preamble(Create_tbl_preambleContext ctx) {
		this.createStatement = new TableCreate();
		this.createStatement.database  = getDB(ctx.table_name());
		this.tableName = this.createStatement.tableName = getTable(ctx.table_name());
	}

	@Override
	public void exitCreate_specifications(Create_specificationsContext ctx) {
		this.createStatement.columns.addAll(this.columnDefs);
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
			} else {
				// BIG TODO: default to database,table,encodings
				colEncoding = "utf8";
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

}