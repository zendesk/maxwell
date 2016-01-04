package com.zendesk.maxwell;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.util.Iterator;
import java.util.List;

public class ColumnWithDefinitionList implements Iterable<ColumnWithDefinition> {
	private final Table table;
	private final Row row;
	private final BitColumn usedColumns;

	public ColumnWithDefinitionList(Table table, Row row, BitColumn usedColumns) {
		this.table = table;
		this.row = row;
		this.usedColumns = usedColumns;
	}

	@Override
	public Iterator<ColumnWithDefinition> iterator() {
		return new ColumnWithDefinitionIterator(table, row, usedColumns);
	}

	class ColumnWithDefinitionIterator implements Iterator<ColumnWithDefinition> {
		private final BitColumn usedColumns;
		private final Iterator<Column> columnIterator;
		private final List<ColumnDef> columnDefList;
		private int index;

		public ColumnWithDefinitionIterator(Table table, Row row, BitColumn usedColumns) {
			this.columnDefList = table.getColumnList();
			this.columnIterator = row.getColumns().iterator();
			this.usedColumns = usedColumns;
			this.index = 0;
		}

		@Override
		public boolean hasNext() {
			return columnIterator.hasNext();
		}

		@Override
		public void remove() { throw new UnsupportedOperationException(); }

		@Override
		public ColumnWithDefinition next() {
			while ( index < usedColumns.getLength() ) {
				if ( usedColumns.get(index) ) {
					ColumnWithDefinition cd = new ColumnWithDefinition(columnIterator.next(), columnDefList.get(index));
					index++;
					return cd;
				} else
					index++;
			}
			return null;
		}
	}

}

