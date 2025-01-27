package com.zendesk.maxwell.schema;

import java.util.ArrayList;
import java.util.List;

import com.zendesk.maxwell.CaseSensitivity;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

public class Database {
	private final String name;
	private final List<Table> tableList;
	private String charset;
	private CaseSensitivity sensitivity;

	public Database(String name, List<Table> tables, String charset) {
		this.name = name;
		if ( tables == null )
			this.tableList = new ArrayList<>();
		else
			this.tableList = tables;
		this.charset = charset;
	}

	public Database(String name, String charset) {
		this(name, null, charset);
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

	public Table findTableOrThrow(String table) throws InvalidSchemaError {
		Table t = findTable(table);
		if ( t == null )
			throw new InvalidSchemaError("Couldn't find table '" + table + "'" + " in database " + this.name);

		return t;
	}

	public boolean hasTable(String name) {
		return findTable(name) != null;
	}

	public void removeTable(String name) {
		Table t = findTable(name);
		if ( t != null )
			tableList.remove(t);
	}

	public Database copy() {
		Database d = new Database(this.name, this.charset);
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
		if ( !Schema.charsetEquals(this.charset, other.getCharset()) ) {
			diffs.add("-- Database " + this.getName() + " had different charset: "
					+ this.getCharset() + " in " + nameA + ", "
					+ other.getCharset() + " in " + nameB);
		}
		diffTableList(diffs, this, other, nameA, nameB, true);
		diffTableList(diffs, other, this, nameB, nameA, false);
	}

	public String getCharset() {
		if ( charset == null ) {
			// TODO: return server-default charset
			return "";
		} else {
		    return charset;
		}
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getName() {
		return name;
	}

	public List<Table> getTableList() {
		return tableList;
	}

	public void addTable(Table table) {
		table.setDatabase(this.name);
		this.tableList.add(table);
	}

	public Table buildTable(String name, String charset, List<ColumnDef> list, List<String> pks) {
		if ( charset == null )
			charset = getCharset(); // inherit database's default charset

		if ( sensitivity == CaseSensitivity.CONVERT_TO_LOWER )
			name = name.toLowerCase();

		Table t = new Table(this.name, name, charset, list, pks);
		this.tableList.add(t);
		return t;
	}

	public Table buildTable(String name, String charset) {
		return buildTable(name, charset, new ArrayList<ColumnDef>(), null);
	}

	public void setSensitivity(CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
	}
}
