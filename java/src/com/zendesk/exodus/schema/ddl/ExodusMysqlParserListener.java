package com.zendesk.exodus.schema.ddl;

import java.util.ArrayList;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;

import com.zendesk.exodus.schema.columndef.ColumnDef;
import com.zendesk.exodus.schema.columndef.StringColumnDef;
import com.zendesk.exodus.schema.ddl.mysqlParser.Add_columnContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Alter_tbl_preambleContext;
import com.zendesk.exodus.schema.ddl.mysqlParser.Col_positionContext;

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
		throw new ExodusSQLSyntaxRrror(node.getText());
	}

	private String unquote(String ident) {
		return ident.replaceAll("^`", "").replaceAll("`$", "");
	}

	@Override
	public void exitAlter_tbl_preamble(Alter_tbl_preambleContext ctx) {
		alterStatement = new TableAlter(currentDatabase);

		if ( ctx.table_name().ID().size() > 1 ) {
			alterStatement.database  = unquote(ctx.table_name().ID(0).getText());
			alterStatement.tableName = unquote(ctx.table_name().ID(1).getText());
		} else {
			alterStatement.tableName = unquote(ctx.table_name().ID(0).getText());
		}
		System.out.println(alterStatement);
	}

	@Override
	public void exitAdd_column(Add_columnContext ctx) {
		String name = unquote(ctx.col_name().getText());
		ColumnDef def = new StringColumnDef(alterStatement.tableName, name, "text", -1, "utf8");
		ColumnPosition p = new ColumnPosition();

		Col_positionContext pctx = ctx.col_position();
		if ( pctx != null ) {
			if ( pctx.FIRST() != null ) {
				p.position = ColumnPosition.Position.FIRST;
			} else if ( pctx.AFTER() != null ) {
				p.position = ColumnPosition.Position.AFTER;
				p.afterColumn = unquote(pctx.ID().getText());
			}
		}

		alterStatement.columnMods.add(new AddColumnMod(name, def, p));
	}
}
