package com.zendesk.maxwell.schema;

import java.util.ArrayList;
import java.util.List;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class Database {
	private final String name;
	private final List<Table> tableList;
	private String encoding;
	private CaseSensitivity sensitivity;

	public Database(String name, List<Table> tables, String encoding) {
		this.name = name;
		if ( tables == null )
			this.tableList = new ArrayList<>();
		else
			this.tableList = tables;
		this.encoding = encoding;
	}

	public Database(String name, String encoding) {
		this(name, null, encoding);
	}

	public List<String> getTableNames() {
		ArrayList<String> names = new ArrayList<String>();
		for ( Table t : this.tableList ) {
			names.add(t.getName());
		}
		return names;
	}

	private boolean compareTableNames(String a, String b) {
		if ( sensitivity == CaseSensitivity.CASE_SENSITIVE )
			return a.equals(b);
		else
			return a.toLowerCase().equals(b.toLowerCase());
	}

	public Table findTable(String name) {
		for ( Table t: this.tableList ) {
			if ( compareTableNames(name, t.getName()))
				return t;
		}
		return null;
	}

	public Database copy() {
		Database d = new Database(this.name, this.encoding);
		for ( Table t: this.tableList ) {
			d.addTable(t.copy());
		}
		return d;
	}

	private void diffTableList(List<String> diffs, Database a, Database b, String nameA, String nameB, boolean recurse) {
		for ( Table t : a.getTableList() ) {
			Table other = b.findTable(t.getName());
			if ( other == null )
				diffs.add("database " + a.getName() + " did not contain table " + t.getName() + " in " + nameB);
			else if ( recurse )
				t.diff(diffs, other, nameA, nameB);
		}
	}

	public void diff(List<String> diffs, Database other, String nameA, String nameB) {
		if ( !this.encoding.toLowerCase().equals(other.getEncoding().toLowerCase()) ) {
			diffs.add("-- Database " + this.getName() + " had different encoding: "
					+ this.getEncoding() + " in " + nameA + ", "
					+ other.getEncoding() + " in " + nameB);
		}
		diffTableList(diffs, this, other, nameA, nameB, true);
		diffTableList(diffs, other, this, nameB, nameA, false);
	}

	public String getEncoding() {
		if ( encoding == null ) {
			// TODO: return server-default encoding
			return "";
		} else {
		    return encoding;
		}
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getName() {
		return name;
	}

	public List<Table> getTableList() {
		return tableList;
	}

	public void addTable(Table table) {
		table.setDatabase(this);
		this.tableList.add(table);
	}

	public Table buildTable(String name, String encoding, List<ColumnDef> list, List<String> pks) {
		if ( encoding == null )
			encoding = getEncoding(); // inherit database's default encoding

		if ( sensitivity == CaseSensitivity.CONVERT_TO_LOWER )
			name = name.toLowerCase();

		Table t = new Table(this, name, encoding, list, pks);
		this.tableList.add(t);
		return t;
	}

	public Table buildTable(String name, String encoding) {
		return buildTable(name, encoding, new ArrayList<ColumnDef>(), null);
	}

	public void setSensitivity(CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
	}
}
