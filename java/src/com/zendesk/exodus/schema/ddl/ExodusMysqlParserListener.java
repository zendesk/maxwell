package com.zendesk.exodus.schema.ddl;

import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.tree.ErrorNode;

import com.zendesk.exodus.schema.columndef.ColumnDef;
import com.zendesk.exodus.schema.ddl.mysqlParser.Add_columnContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Alter_tbl_preambleContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Charset_defContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Col_positionContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Data_typeContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Int_flagsContext;

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

class RemoveColumnMod extends ColumnMod {
	public RemoveColumnMod(String name) {
		super(name);
	}
}

class ExodusSQLSyntaxRrror extends RuntimeException {
	public ExodusSQLSyntaxRrror(String message) {
		super(message);
	}
}

public class ExodusMysqlParserListener extends mysqlBaseListener {
	private TableAlter alterStatement;
	private final LinkedList<ColumnDef> columnDefs = new LinkedList<>();

	public TableAlter getAlterStatement() {
		return alterStatement;
	}

	private final String currentDatabase;

	ExodusMysqlParserListener(String currentDatabase)  {
		this.currentDatabase = currentDatabase;
	}

	@Override
	public void visitErrorNode(ErrorNode node) {
		this.alterStatement = null;
		System.out.println(node.getParent().toStringTree(new mysqlParser(null)));
		throw new ExodusSQLSyntaxRrror(node.getText());
	}

	private String unquote(String ident) {
		return ident.replaceAll("^`", "").replaceAll("`$", "");
	}

	@Override
	public void exitAlter_tbl_preamble(Alter_tbl_preambleContext ctx) {
		alterStatement = new TableAlter(currentDatabase);

		if ( ctx.table_name().id().size() > 1 ) {
			alterStatement.database  = unquote(ctx.table_name().id(0).getText());
			alterStatement.tableName = unquote(ctx.table_name().id(1).getText());
		} else {
			alterStatement.tableName = unquote(ctx.table_name().id(0).getText());
		}
		System.out.println(alterStatement);
	}

	private boolean isSigned(List<Int_flagsContext> flags) {
		for ( Int_flagsContext flag : flags ) {
			if ( flag.UNSIGNED() != null ) {
				return false;
			}
		}
		return true;
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
				colEncoding = charsetDef.character_set(0).IDENT().getText();
			} else {
				// BIG TODO: default to database,table,encodings
				colEncoding = "utf8";
			}
		}

		ColumnDef c = ColumnDef.build(alterStatement.tableName,
					                   name,
					                   colEncoding,
					                   colType.toLowerCase(),
					                   -1,
					                   signed);
		this.columnDefs.add(c);
	}

	@Override
	public void exitAdd_column(Add_columnContext ctx) {
		ColumnPosition p = new ColumnPosition();

		Col_positionContext pctx = ctx.col_position();
		if ( pctx != null ) {
			if ( pctx.FIRST() != null ) {
				p.position = ColumnPosition.Position.FIRST;
			} else if ( pctx.AFTER() != null ) {
				p.position = ColumnPosition.Position.AFTER;
				p.afterColumn = unquote(pctx.id().getText());
			}
		}

		ColumnDef c = this.columnDefs.removeFirst();
		alterStatement.columnMods.add(new AddColumnMod(c.getName(), c, p));
	}

	@Override public void exitAdd_column_parens(mysqlParser.Add_column_parensContext ctx) {
		while ( this.columnDefs.size() > 0 ) {
			ColumnDef c = this.columnDefs.removeFirst();
			// unable to choose a position in this form
			alterStatement.columnMods.add(new AddColumnMod(c.getName(), c, new ColumnPosition()));
		}
	}

}
