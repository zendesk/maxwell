package com.zendesk.maxwell.schema;

import java.util.ArrayList;
import java.util.List;

import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

public class Table {
	private final List<ColumnDef> columnList;
	int pkIndex;
	private String name;

	private Database database;
	private final String encoding;

	public Table(Database d, String name, String encoding, List<ColumnDef> list) {
		this.database = d;
		this.name = name;
		this.encoding = encoding;
		this.columnList = list;
		renumberColumns();
	}

	public List<ColumnDef> getColumnList() {
		return columnList;
	}

	public String getName() {
		return this.name;
	}

	public int findColumnIndex(String name) {
		int i = 0;
		for(ColumnDef c : columnList) {
			if ( c.getName().equals(name) )
				return i;
			i++;

		}
		return -1;
	}

	private ColumnDef findColumn(String name) {
		for (ColumnDef c : columnList )  {
			if ( c.getName().equals(name) )
				return c;
		}

		return null;
	}


	public int getPKIndex() {
		return this.pkIndex;
	}

	public Database getDatabase() {
		return database;
	}

	public Table copy() {
		ArrayList<ColumnDef> list = new ArrayList<>();

		for ( ColumnDef c : columnList ) {
			list.add(c.copy());
		}

		return new Table(database, name, encoding, list);
	}

	public void rename(String tableName) {
		this.name = tableName;
	}

	private void diffColumnList(List<String> diffs, Table a, Table b, String nameA, String nameB) {
		for ( ColumnDef column : a.getColumnList() ) {
			ColumnDef other = b.findColumn(column.getName());
			if ( other == null )
				diffs.add(b.fullName() + " is missing column " + column.getName() + " in " + nameB);
			else {
                String colName = a.fullName() + ".`" + column.getName() + "` ";
				if ( !column.getType().equals(other.getType()) ) {
					diffs.add(colName + "has a type mismatch, "
									  + column.getType()
									  + " vs "
									  + other.getType()
									  + " in " + nameB);
				} else if ( column.getPos() != other.getPos() ) {
					diffs.add(colName + "has a position mismatch, "
									  + column.getPos()
									  + " vs "
									  + other.getPos()
									  + " in " + nameB);
				}
			}
		}
	}

	public String fullName() {
		return "`" + this.database.getName() + "`." + this.name + "`";
	}

	public void diff(List<String> diffs, Table other, String nameA, String nameB) {
		if ( !this.getEncoding().equals(other.getEncoding()) ) {
			diffs.add(this.fullName() + " differs in encoding: "
					  + nameA + " is " + this.getEncoding() + " but "
					  + nameB + " is " + other.getEncoding());
		}
		diffColumnList(diffs, this, other, nameA, nameB);
		diffColumnList(diffs, other, this, nameB, nameA);
	}

	private void renumberColumns() {
		int i = 0 ;
		for ( ColumnDef c : columnList ) {
			c.setPos(i++);
		}
	}

	public void setDefaultColumnEncodings() {
		for ( ColumnDef c : columnList ) {
			if ( c instanceof StringColumnDef ) {
				((StringColumnDef) c).setDefaultEncoding(this.getEncoding());
			}
		}
	}

	public void addColumn(int index, ColumnDef definition) {
		this.columnList.add(index, definition);
		renumberColumns();
	}

	public void removeColumn(int idx) {
		this.columnList.remove(idx);
		renumberColumns();
	}

	public void setDatabase(Database database) {
		this.database = database;
	}

	public String getEncoding() {
		if ( encoding == null ) {
			return this.database.getEncoding();
		} else {
		    return encoding;
		}
	}

}
