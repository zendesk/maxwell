package com.zendesk.maxwell.schema;

import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.zendesk.maxwell.schema.columndef.ColumnDef;


public class TableColumnList implements Iterable<ColumnDef> {
	// reduce count of duplicate ArrayLists/Sets for column lists by providing mutability for the class
	// through references to an internal immutable object that gets interned. This greatly reduces overhead for
	// table definitions that are duplicated across databases
	private ImmutableColumnList columnList;

	public TableColumnList(List<ColumnDef> columns) {
		this.columnList = ImmutableColumnList.create(columns);
	}

	public Iterator<ColumnDef> iterator() {
		return columnList.getColumns().iterator();
	}

	public List<ColumnDef> getList() {
		return columnList.getColumns();
	}

	public synchronized Set<String> columnNames() {
		return columnList.getColumnNames();
	}

	public synchronized int indexOf(String name) {
		return indexOf(columnList.getColumns(), name);
	}

	private synchronized int indexOf(List<ColumnDef> columns, String name) {
		String lcName = name.toLowerCase();

		for ( int i = 0 ; i < columns.size(); i++ ) {
			if ( columns.get(i).getName().toLowerCase().equals(lcName) )
				return i;
		}
		return -1;
	}

	public ColumnDef findByName(String name) {
		List<ColumnDef> columns = columnList.getColumns();
		int index = indexOf(columns, name);
		if ( index == -1 )
			return null;
		else
			return columns.get(index);
	}

	public synchronized void add(int index, ColumnDef definition) {
		List<ColumnDef> columns = columnList.getColumns();
		ArrayList<ColumnDef> tempList = new ArrayList<>(columns.size() + 1);
		tempList.addAll(columns);
		tempList.add(index, definition);
		columnList = ImmutableColumnList.create(tempList);
	}

	public synchronized void addAll(List<ColumnDef> columnDefs) {
		columnList = ImmutableColumnList.create(columnDefs);
	}

	public synchronized void replace(int index, ColumnDef definition) {
		List<ColumnDef> columns = columnList.getColumns();
		ArrayList<ColumnDef> tempList = new ArrayList<>(columns.size());
		tempList.addAll(columns);
		tempList.set(index, definition);
		columnList = ImmutableColumnList.create(tempList);
	}

	public synchronized ColumnDef remove(int index) {
		List<ColumnDef> columns = columnList.getColumns();
		ArrayList<ColumnDef> tempList = new ArrayList<>(columns.size());
		tempList.addAll(columns);
		ColumnDef c = tempList.remove(index);
		columnList = ImmutableColumnList.create(tempList);
		return c;
	}

	public synchronized ColumnDef get(int index) {
		return columnList.getColumns().get(index);
	}

	public int size() {
		return columnList.getColumns().size();
	}

	private static final class ImmutableColumnList {
		private static final Interner<ImmutableColumnList> INTERNER = Interners.newWeakInterner();

		private final List<ColumnDef> columns;
		private Set<String> columnNames; // not part of equals because it's derived statically

		private ImmutableColumnList(List<ColumnDef> columns) {
			ImmutableList.Builder<ColumnDef> builder = ImmutableList.builderWithExpectedSize(columns.size());
			int i = 0;
			for (ColumnDef column : columns) {
				builder.add(column.withPos((short) i++));
			}
			this.columns = builder.build();
		}

		public static ImmutableColumnList create(List<ColumnDef> columns) {
			return INTERNER.intern(new ImmutableColumnList(columns));
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof ImmutableColumnList) {
				ImmutableColumnList other = (ImmutableColumnList) o;
				return columns.equals(other.columns);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return columns.hashCode();
		}

		public List<ColumnDef> getColumns() {
			return columns;
		}

		public Set<String> getColumnNames() {
			if ( columnNames == null ) {
				columnNames = generateColumnNames();
			}
			return columnNames;
		}

		private Set<String> generateColumnNames() {
			ImmutableSet.Builder<String> setBuilder = ImmutableSet.builderWithExpectedSize(columns.size());
			for ( ColumnDef cf : columns ) {
				setBuilder.add(cf.getName().toLowerCase().intern());
			}
			return setBuilder.build();
		}
	}
}



