package com.zendesk.maxwell.schema.columndef;

public abstract class ColumnDef {
	protected final String tableName;
	protected final String name;
	protected final String type;
	protected String[] enumValues;
	private int pos;
	public boolean signed;
	public String encoding;

	public ColumnDef(String tableName, String name, String type, int pos) {
		this.tableName = tableName;
		this.name = name.toLowerCase();
		this.type = type;
		this.pos = pos;
		this.signed = false;
	}

	public abstract boolean matchesMysqlType(int type);
	public abstract String toSQL(Object value);

	public Object asJSON(Object value) {
		return value;
	}

	public ColumnDef copy() {
		return build(this.tableName, this.name, this.encoding, this.type, this.pos, this.signed, this.enumValues);
	}

	public static ColumnDef build(String tableName, String name, String encoding, String type, int pos, boolean signed, String enumValues[]) {
		switch(type) {
		case "tinyint":
		case "smallint":
		case "mediumint":
		case "int":
			return new IntColumnDef(tableName, name, type, pos, signed);
		case "bigint":
			return new BigIntColumnDef(tableName, name, type, pos, signed);
		case "tinytext":
		case "text":
		case "mediumtext":
		case "longtext":
		case "varchar":
		case "char":
			return new StringColumnDef(tableName, name, type, pos, encoding);
		case "tinyblob":
		case "blob":
		case "mediumblob":
		case "longblob":
		case "binary":
		case "varbinary":
			return new StringColumnDef(tableName, name, type, pos, "binary");
		case "float":
		case "double":
			return new FloatColumnDef(tableName, name, type, pos);
		case "decimal":
			return new DecimalColumnDef(tableName, name, type, pos);
		case "date":
			return new DateColumnDef(tableName, name, type, pos);
		case "datetime":
		case "timestamp":
			return new DateTimeColumnDef(tableName, name, type, pos);
		case "year":
			return new YearColumnDef(tableName, name, type, pos);
		case "time":
			return new TimeColumnDef(tableName, name, type, pos);
		case "enum":
			return new EnumColumnDef(tableName, name, type, pos, enumValues);
		case "set":
			return new SetColumnDef(tableName, name, type, pos, enumValues);
		case "bit":
			return new BitColumnDef(tableName, name, type, pos);
		default:
			throw new IllegalArgumentException("unsupported column type " + type);
		}
	}

	static public String unalias_type(String type, boolean longStringFlag) {
		if ( longStringFlag ) {
			switch (type) {
				case "varchar":
					return "mediumtext";
				case "varbinary":
					return "mediumblob";
				case "binary":
					return "mediumtext";
			}
		}

		switch(type) {
			case "bool":
			case "boolean":
			case "int1":
				return "tinyint";
			case "int2":
				return "smallint";
			case "int3":
				return "mediumint";
			case "int4":
			case "integer":
				return "int";
			case "int8":
				return "bigint";
			case "real":
			case "numeric":
				return "double";
			default:
				return type;
		}
	}

	public String getName() {
		return name;
	}

	public String getTableName() {
		return tableName;
	}

	public String getType() {
		return type;
	}

	public int getPos() {
		return pos;
	}

	public void setPos(int i) {
		this.pos = i;
	}

	public String getEncoding() {
		return this.encoding;
	}

	public boolean getSigned() {
		return this.signed;
	}

	public String[] getEnumValues() {
		return enumValues;
	}

}
