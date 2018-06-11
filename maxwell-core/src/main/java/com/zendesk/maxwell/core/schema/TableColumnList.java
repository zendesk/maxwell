package com.zendesk.maxwell.core.schema;

import com.zendesk.maxwell.core.schema.columndef.ColumnDef;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class TableColumnList implements Iterable<ColumnDef> {
	private final List<ColumnDef> columns;
	private HashMap<String, Integer> columnOffsetMap;

	public TableColumnList(List<ColumnDef> columns) {
		this.columns = columns;
		initColumnOffsetMap();
		renumberColumns();
	}

	public Iterator<ColumnDef> iterator() {
		return columns.iterator();
	}

	public List<ColumnDef> getList() {
		return columns;
	}

	public synchronized int indexOf(String name) {
		String lcName = name.toLowerCase();

		if ( this.columnOffsetMap.containsKey(lcName) ) {
			return this.columnOffsetMap.get(lcName);
		} else {
			return -1;
		}
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
		initColumnOffsetMap();
		renumberColumns();
	}

	public synchronized ColumnDef remove(int index) {
		ColumnDef c = columns.remove(index);
		initColumnOffsetMap();
		renumberColumns();
		return c;
	}

	public synchronized ColumnDef get(int index) {
		return columns.get(index);
	}

	public int size() {
		return columns.size();
	}

	private void initColumnOffsetMap() {
		this.columnOffsetMap = new HashMap<>();
		int i = 0;

		for(ColumnDef c : columns) {
			this.columnOffsetMap.put(c.getName().toLowerCase(), i++);
		}
	}

	private void renumberColumns() {
		int i = 0 ;
		for ( ColumnDef c : columns ) {
			c.setPos(i++);
		}
	}
}



