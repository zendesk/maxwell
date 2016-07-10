package com.zendesk.maxwell.schema;

import java.util.*;

import com.zendesk.maxwell.schema.ddl.InvalidSchemaError;

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
	private List<ColumnDef> columnList;
	public String charset;
	private List<String> pkColumnNames;
	private List<String> normalizedPKColumnNames;

	private HashMap<String, Integer> columnOffsetMap;
	@JsonIgnore
	public int pkIndex;

	public Table() { }
	public Table(String database, String name, String charset, List<ColumnDef> list, List<String> pks) {
		this.database = database;
		this.name = name;
		this.charset = charset;
		this.setColumnList(list);

		if ( pks == null )
			pks = new ArrayList<String>();

		this.setPKList(pks);
	}

	@JsonProperty("columns")
	public List<ColumnDef> getColumnList() {
		return columnList;
	}

	@JsonProperty("columns")
	public void setColumnList(List<ColumnDef> list) {
		this.columnList = list;
		renumberColumns();
	}

	@JsonIgnore
	public List<StringColumnDef> getStringColumns() {
		ArrayList<StringColumnDef> list = new ArrayList<>();
		for ( ColumnDef c : columnList ) {
			if ( c instanceof StringColumnDef )
				list.add((StringColumnDef) c);
		}
		return list;
	}

	public String getName() {
		return this.name;
	}

	private void initColumnOffsetMap() {
		if ( this.columnOffsetMap != null )
			return;

		this.columnOffsetMap = new HashMap<>();
		int i = 0;

		for(ColumnDef c : columnList) {
			this.columnOffsetMap.put(c.getName().toLowerCase(), i++);
		}
	}

	public int findColumnIndex(String name) {
		String lcName = name.toLowerCase();
		initColumnOffsetMap();

		if ( this.columnOffsetMap.containsKey(lcName) ) {
			return this.columnOffsetMap.get(lcName);
		} else {
			return -1;
		}
	}

	public ColumnDef findColumn(String name) {
		int index = findColumnIndex(name);
		if ( index == -1 )
			return null;
		else
			return columnList.get(index);
	}

	public ColumnDef findColumnOrThrow(String name) throws InvalidSchemaError {
		ColumnDef c = findColumn(name);
		if ( c == null )
			throw new InvalidSchemaError("couldn't find column " + name + " in table " + this.name);

		return c;
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

		for ( ColumnDef c : columnList ) {
			list.add(c);
		}

		return new Table(database, name, charset, list, pkColumnNames);
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

				if ( column instanceof EnumeratedColumnDef ) {
					EnumeratedColumnDef enumA, enumB;
					enumA = (EnumeratedColumnDef) column;
					enumB = (EnumeratedColumnDef) other;
					if ( !Arrays.deepEquals(enumA.getEnumValues(), enumB.getEnumValues()) ) {
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

					if ( !Objects.equals(stringA.getCharset(), stringB.getCharset()) ) {
						diffs.add(colName + "has an charset mismatch, "
								+ "'" + stringA.getCharset() + "'"
								+ " vs "
								+ "'" + stringB.getCharset() + "'"
								+ " in " + nameB);
					}

				}
			}
		}
	}

	public String fullName() {
		return "`" + this.database + "`." + this.name + "`";
	}

	public void diff(List<String> diffs, Table other, String nameA, String nameB) {
		if ( !this.getCharset().equals(other.getCharset()) ) {
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

	private void renumberColumns() {
		int i = 0 ;
		for ( ColumnDef c : columnList ) {
			c.setPos(i++);
		}
	}

	public void setDefaultColumnCharsets() {
		for ( StringColumnDef c : getStringColumns() ) {
			c.setDefaultCharset(this.getCharset());
		}
	}

	public void addColumn(int index, ColumnDef definition) {
		this.columnList.add(index, definition);
		this.columnOffsetMap = null;
		renumberColumns();
	}

	public void addColumn(ColumnDef defintion) {
		addColumn(this.columnList.size(), defintion);
	}

	public void removeColumn(int idx) {
		removePKColumn(columnList.get(idx).getName());

		this.columnList.remove(idx);
		this.columnOffsetMap = null;

		renumberColumns();
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCharset() {
		return charset;
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
		this.pkColumnNames = pkColumnNames;
		this.normalizedPKColumnNames = null;
	}

	private synchronized void removePKColumn(String name) {
		int pkIndex = getPKList().indexOf(name);
		if ( pkIndex != -1 ) {
			this.pkColumnNames.remove(pkIndex);
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
