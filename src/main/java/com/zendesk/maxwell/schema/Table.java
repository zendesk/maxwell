package com.zendesk.maxwell.schema;

import java.util.*;
import java.util.stream.Collectors;

import com.zendesk.maxwell.schema.ddl.DeferredPositionUpdate;
import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;
import com.zendesk.maxwell.schema.ddl.ColumnPosition;

import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.BigIntColumnDef;
import com.zendesk.maxwell.schema.columndef.EnumeratedColumnDef;
import org.apache.commons.lang3.StringUtils;

import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.StringColumnDef;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Table {
	public String database;
	@JsonProperty("table")
	public String name;
	private TableColumnList columns;
	public String charset;
	private List<String> pkColumnNames;
	private List<String> normalizedPKColumnNames;

	@JsonIgnore
	public int pkIndex;

	public Table() { }
	public Table(String database, String name, String charset, List<ColumnDef> list, List<String> pks) {
		this.database = database.intern();
		this.name = name.intern();
		this.charset = charset;
		if ( this.charset != null )
			this.charset = this.charset.intern();

		this.setColumnList(list);

		if ( pks == null )
			pks = new ArrayList<String>();

		this.setPKList(pks);
	}

	@JsonProperty("table")
	public void setTable(String name) {
		this.name = name.intern();
	}

	@JsonProperty("columns")
	public List<ColumnDef> getColumnList() {
		return columns.getList();
	}

	@JsonIgnore
	public Set<String> getColumnNames() {
		return columns.columnNames();
	}

	@JsonProperty("columns")
	public void setColumnList(List<ColumnDef> list) {
		this.columns = new TableColumnList(list);
	}

	@JsonIgnore
	public List<StringColumnDef> getStringColumns() {
		ArrayList<StringColumnDef> list = new ArrayList<>();
		for ( ColumnDef c : columns ) {
			if ( c instanceof StringColumnDef )
				list.add((StringColumnDef) c);
		}
		return list;
	}

	public String getName() {
		return this.name;
	}

	public short findColumnIndex(String name) {
		return (short) columns.indexOf(name);
	}


	public ColumnDef findColumn(String name) {
		return columns.findByName(name);
	}

	public ColumnDef findColumn(int index) {
		return columns.get(index);
	}

	@JsonIgnore
	public int getPKIndex() {
		return this.pkIndex;
	}

	public String getDatabase() {
		return database;
	}

	public Table copy() {
		ArrayList<ColumnDef> list = new ArrayList<>();
		ArrayList<String> pkList = new ArrayList<>();

		for ( ColumnDef c : columns ) {
			list.add(c);
		}

		for ( String s : pkColumnNames ) {
			pkList.add(s);
		}

		return new Table(database, name, charset, list, pkList);
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
					return;
				} else if ( column.getPos() != other.getPos() ) {
					diffs.add(colName + "has a position mismatch, "
									  + column.getPos()
									  + " vs "
									  + other.getPos()
									  + " in " + nameB);
				}

				if ( column instanceof EnumeratedColumnDef ) {
					EnumeratedColumnDef enumA, enumB;
					enumA = (EnumeratedColumnDef) column;
					enumB = (EnumeratedColumnDef) other;
					if ( !enumA.getEnumValues().equals(enumB.getEnumValues()) ) {
						diffs.add(colName + "has an enum value mismatch, "
								+ StringUtils.join(enumA.getEnumValues(), ",")
								+ " vs "
								+ StringUtils.join(enumB.getEnumValues(), ",")
								+ " in " + nameB);
					}
				}

				if ( column instanceof StringColumnDef ) {
					StringColumnDef stringA, stringB;
					stringA = (StringColumnDef) column;
					stringB = (StringColumnDef) other;

					if ( !Schema.charsetEquals(stringA.getCharset(), stringB.getCharset()) ) {
						diffs.add(colName + "has an charset mismatch, "
								+ "'" + stringA.getCharset() + "'"
								+ " vs "
								+ "'" + stringB.getCharset() + "'"
								+ " in " + nameB);
					}

				}

				if ( column instanceof IntColumnDef ) {
					boolean signedA, signedB;
					signedA = ((IntColumnDef) column).isSigned();
					signedB = ((IntColumnDef) other).isSigned();

					if ( signedA != signedB )
						diffs.add(colName + "has a signedness mismatch, "
								+ "'" + signedA + "'"
								+ " vs "
								+ "'" + signedB + "'"
								+ " in " + nameB);
				}

				if ( column instanceof BigIntColumnDef ) {
					boolean signedA, signedB;
					signedA = ((BigIntColumnDef) column).isSigned();
					signedB = ((BigIntColumnDef) other).isSigned();

					if ( signedA != signedB )
						diffs.add(colName + "has a signedness mismatch, "
								+ "'" + signedA + "'"
								+ " vs "
								+ "'" + signedB + "'"
								+ " in " + nameB);
				}
			}
		}
	}

	public String fullName() {
		return "`" + this.database + "`." + this.name + "`";
	}

	public void diff(List<String> diffs, Table other, String nameA, String nameB) {
		if ( !Schema.charsetEquals(this.charset, other.getCharset()) ) {
			diffs.add(this.fullName() + " differs in charset: "
					  + nameA + " is " + this.getCharset() + " but "
					  + nameB + " is " + other.getCharset());
		}

		if ( !this.getPKString().equals(other.getPKString())) {
			diffs.add(this.fullName() + " differs in PKs: "
					  + nameA + " is " + this.getPKString() + " but "
					  + nameB + " is " + other.getPKString());
		}

		if ( !this.getName().equals(other.getName()) ) {
			diffs.add(this.fullName() + " differs in name: "
					  + nameA + " is " + this.getName() + " but "
					  + nameB + " is " + other.getName());
		}

		diffColumnList(diffs, this, other, nameA, nameB);
		diffColumnList(diffs, other, this, nameB, nameA);
	}

	public void setDefaultColumnCharsets() {
		String newCharset = this.getCharset();
		for ( StringColumnDef c : getStringColumns() ) {
			int index = c.getPos();
			columns.replace(index, c.withDefaultCharset(newCharset));
		}
	}

	public void addColumn(int index, ColumnDef definition) {
		columns.add(index, definition);
	}

	public void addColumn(ColumnDef definition) {
		columns.add(columns.size(), definition);
	}

	public void addColumns(List<ColumnDef> definitions) {
		columns.addAll(definitions);
	}

	public void removeColumn(int idx) {
		ColumnDef toRemove = columns.get(idx);
		removePKColumn(toRemove.getName());
		columns.remove(idx);
	}

	public void renameColumn(int idx, String name) throws InvalidSchemaError {
		ColumnDef oldColumn = columns.get(idx);
		renamePKColumn(oldColumn.getName(), name);

		ColumnDef column = columns.get(idx).withName(name);
		columns.replace(idx, column);
	}

	public void replaceColumn(int idx, ColumnDef definition) throws InvalidSchemaError {
		columns.replace(idx, definition);
	}

	public void changeColumn(int idx, ColumnPosition position, ColumnDef definition, List<DeferredPositionUpdate> deferred) throws InvalidSchemaError {
		// when we go to rename the PK column, we need to make sure the old column name
		// is still there for (for normalization of pk-columns).
		ColumnDef oldColumn = columns.get(idx);
		renamePKColumn(oldColumn.getName(), definition.getName());

		columns.remove(idx);

		int index = position.index(this, idx);
		if ( index == ColumnPosition.AFTER_NOT_FOUND) {
			deferred.add(new DeferredPositionUpdate(definition.getName(), position));
			index = 0;
		}

		columns.add(index, definition);
	}

	public void moveColumn(String name, ColumnPosition position) throws InvalidSchemaError {
		int idx = columns.indexOf(name);
		ColumnDef def = columns.remove(idx);
		int newIndex = position.index(this, idx);

		if ( newIndex == ColumnPosition.AFTER_NOT_FOUND)
			throw new InvalidSchemaError("Couldn't find column " + position.afterColumn + " to place after");

		columns.add(newIndex, def);
	}

	public void setDatabase(String database) {
		this.database = database.intern();
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
		if ( this.charset != null )
			this.charset = charset.intern();
	}

	@JsonProperty("primary-key")
	public List<String> getPKList() {
		return normalizedColumnNames();
	}

	@JsonIgnore
	public String getPKString() {
		if ( this.pkColumnNames != null )
			return StringUtils.join(pkColumnNames.iterator(), ",");
		else
			return null;
	}

	@JsonProperty("primary-key")
	public synchronized void setPKList(List<String> pkColumnNames) {
		this.pkColumnNames = pkColumnNames.stream().map((n) -> n.intern()).collect(Collectors.toList());
		this.normalizedPKColumnNames = null;
	}

	private synchronized void removePKColumn(String name) {
		int pkIndex = getPKList().indexOf(name);
		if ( pkIndex != -1 ) {
			this.pkColumnNames.remove(pkIndex);
			this.normalizedPKColumnNames = null;
		}
	}

	private synchronized void renamePKColumn(String oldName, String newName) {
		int pkIndex = getPKList().indexOf(oldName);
		if ( pkIndex != -1 ) {
			this.pkColumnNames.set(pkIndex, newName);
			this.normalizedPKColumnNames = null;
		}
	}
	private synchronized List<String> normalizedColumnNames() {
		/*
		   primary keys may come in with different casing than the column names.
		   convert the list of primary keys to match the column casing.

		   we do this normalization lazily, as when a Table object is being deserialized
		   from JSON, there may be no column definitions present when the setPKList() function is called.
		   ugly!
		 */
		if ( this.normalizedPKColumnNames == null ) {
			this.normalizedPKColumnNames = new ArrayList<>(this.pkColumnNames.size());

			for (String name : pkColumnNames) {
				ColumnDef cd = findColumn(name);

				if ( cd == null )
					throw new RuntimeException("Couldn't find column for primary-key: " + name);

				this.normalizedPKColumnNames.add(cd.getName());
			}
		}
		return this.normalizedPKColumnNames;
	}
}
