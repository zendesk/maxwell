package com.zendesk.maxwell.schema;

import java.util.*;

import com.zendesk.maxwell.schema.columndef.ColumnDef;


public class TableColumnList implements Iterable<ColumnDef> {
	private final List<ColumnDef> columns;
	private Set<String> columnNames;

	public TableColumnList(List<ColumnDef> columns) {
		this.columns = columns;
		renumberColumns();
	}

	public Iterator<ColumnDef> iterator() {
		return columns.iterator();
	}

	public List<ColumnDef> getList() {
		return columns;
	}

	public synchronized Set<String> columnNames() {
		if ( columnNames == null ) {
			columnNames = new HashSet<>();
			for ( ColumnDef cf : columns )
				columnNames.add(cf.getName().toLowerCase().intern());
		}
		return columnNames;
	}

	public synchronized int indexOf(String name) {
		String lcName = name.toLowerCase();

		for ( int i = 0 ; i < columns.size(); i++ ) {
			if ( columns.get(i).getName().toLowerCase().equals(lcName) )
				return i;
		}
		return -1;
	}

	public ColumnDef findByName(String name) {
		int index = indexOf(name);
		if ( index == -1 )
			return null;
		else
			return columns.get(index);
	}

	public synchronized void add(int index, ColumnDef definition) {
		columns.add(index, definition);

		if ( columnNames != null )
			columnNames.add(definition.getName().toLowerCase());

		renumberColumns();
	}

	public synchronized ColumnDef remove(int index) {
		ColumnDef c = columns.remove(index);

		if ( columnNames != null )
			columnNames.remove(c.getName().toLowerCase());
		renumberColumns();
		return c;
	}

	public synchronized ColumnDef get(int index) {
		return columns.get(index);
	}

	public int size() {
		return columns.size();
	}

	private void renumberColumns() {
		short i = 0 ;
		for ( ColumnDef c : columns ) {
			c.setPos(i++);
		}
	}
}



