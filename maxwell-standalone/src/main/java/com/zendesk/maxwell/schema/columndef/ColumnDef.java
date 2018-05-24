package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonSerialize(using=ColumnDefSerializer.class)
@JsonDeserialize(using=ColumnDefDeserializer.class)

public abstract class ColumnDef {
	protected String name;
	protected String type;

	protected int pos;

	public ColumnDef() { }
	public ColumnDef(String name, String type, int pos) {
		this.name = name;
		this.type = type;
		this.pos = pos;
	}

	public abstract String toSQL(Object value);

	public Object asJSON(Object value) {
		return value;
	}

	public static ColumnDef build(String name, String charset, String type, int pos, boolean signed, String enumValues[], Long columnLength) {
		switch(type) {
		case "tinyint":
		case "smallint":
		case "mediumint":
		case "int":
			return new IntColumnDef(name, type, pos, signed);
		case "bigint":
			return new BigIntColumnDef(name, type, pos, signed);
		case "tinytext":
		case "text":
		case "mediumtext":
		case "longtext":
		case "varchar":
		case "char":
			return new StringColumnDef(name, type, pos, charset);
		case "tinyblob":
		case "blob":
		case "mediumblob":
		case "longblob":
		case "binary":
		case "varbinary":
			return new StringColumnDef(name, type, pos, "binary");
		case "geometry":
		case "geometrycollection":
		case "linestring":
		case "multilinestring":
		case "multipoint":
		case "multipolygon":
		case "polygon":
		case "point":
			return new GeometryColumnDef(name, type, pos);
		case "float":
		case "double":
			return new FloatColumnDef(name, type, pos);
		case "decimal":
			return new DecimalColumnDef(name, type, pos);
		case "date":
			return new DateColumnDef(name, type, pos);
		case "datetime":
		case "timestamp":
			return new DateTimeColumnDef(name, type, pos, columnLength);
		case "time":			
			return new TimeColumnDef(name, type, pos, columnLength);
		case "year":
			return new YearColumnDef(name, type, pos);
		case "enum":
			return new EnumColumnDef(name, type, pos, enumValues);
		case "set":
			return new SetColumnDef(name, type, pos, enumValues);
		case "bit":
			return new BitColumnDef(name, type, pos);
		case "json":
			return new JsonColumnDef(name, type, pos);

		default:
			throw new IllegalArgumentException("unsupported column type " + type);
		}
	}

	static private String charToByteType(String type) {
		switch (type) {
			case "char":
			case "character":
				return "binary";
			case "varchar":
			case "varying":
				return "varbinary";
			case "tinytext":
				return "tinyblob";
			case "text":
				return "blob";
			case "mediumtext":
				return "mediumblob";
			case "longtext":
				return "longblob";
			case "long":
				return "mediumblob";
			default:
				throw new RuntimeException("Unknown type with BYTE flag: " + type);
		}
	}

	static public String unalias_type(String type, boolean longStringFlag, Long columnLength, boolean byteFlagToStringColumn) {
		if ( byteFlagToStringColumn )
			type = charToByteType(type);

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
			case "character":
			case "nchar":
				return "char";
			case "text":
			case "blob":
				if ( columnLength == null )
					return type;

				if ( columnLength < (1 << 8) )
					return "tiny" + type;
				else if ( columnLength < ( 1 << 16) )
					return type;
				else if ( columnLength < ( 1 << 24) )
					return "medium" + type;
				else
					return "long" + type;
			case "nvarchar":
			case "varying":
				return "varchar";
			case "bool":
			case "boolean":
			case "int1":
				return "tinyint";
			case "int2":
				return "smallint";
			case "int3":
			case "middleint":
				return "mediumint";
			case "int4":
			case "integer":
				return "int";
			case "int8":
			case "serial":
				return "bigint";
			case "float4":
				return "float";
			case "real":
			case "float8":
				return "double";
			case "numeric":
			case "fixed":
				return "decimal";
			case "long":
				return "mediumtext";
			default:
				return type;
		}
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
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
}
