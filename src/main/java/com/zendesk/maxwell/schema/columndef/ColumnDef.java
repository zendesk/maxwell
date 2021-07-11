package com.zendesk.maxwell.schema.columndef;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.util.DynamicEnum;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * This class is immutable, all subclasses must be immutable and implement equals and hashCode and call this
 * class's respective methods if the subclass has any member variables. Failure to do so will lead difficult
 * to debug errors as these class instances are interned. Subclasses may use {@link #cloneSelfAndSet} to
 * follow clone/modify/intern pattern for maintaining interface immutability.
 *
 */
@JsonSerialize(using=ColumnDefSerializer.class)
@JsonDeserialize(using=ColumnDefDeserializer.class)
public abstract class ColumnDef implements Cloneable {
	protected static final Interner INTERNER = Interners.newWeakInterner();
	private static final DynamicEnum dynamicEnum = new DynamicEnum(Byte.MAX_VALUE);
	private String name;
	private final byte type;
	private short pos;

	protected ColumnDef(String name, String type, short pos) {
		this.name = name;
		this.pos = pos;
		this.type = (byte) dynamicEnum.get(type);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ColumnDef && o.getClass() == getClass()) {
			ColumnDef other = (ColumnDef) o;
			return Objects.equals(name, other.name)
					&& Objects.equals(pos, other.pos)
					&& Objects.equals(type, other.type);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, type, pos);
	}

	public abstract String toSQL(Object value) throws ColumnDefCastException;

	protected <T extends ColumnDef> Interner<T> getInterner() {
		// maintain default interner
		return (Interner<T>) INTERNER;
	}

	@Deprecated
	public Object asJSON(Object value) throws ColumnDefCastException {
		return asJSON(value, new MaxwellOutputConfig());
	}

	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return value;
	}

	public ColumnDef clone() {
		try {
			return (ColumnDef) super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	public static ColumnDef build(String name, String charset, String type, short pos, boolean signed, String enumValues[], Long columnLength) {
		name = name.intern();
		if ( charset != null )
			charset = charset.intern();

		switch(type) {
		case "tinyint":
		case "smallint":
		case "mediumint":
		case "int":
			return IntColumnDef.create(name, type, pos, signed);
		case "bigint":
			return BigIntColumnDef.create(name, type, pos, signed);
		case "tinytext":
		case "text":
		case "mediumtext":
		case "longtext":
		case "varchar":
		case "char":
			return StringColumnDef.create(name, type, pos, charset);
		case "tinyblob":
		case "blob":
		case "mediumblob":
		case "longblob":
		case "binary":
		case "varbinary":
			return StringColumnDef.create(name, type, pos, "binary");
		case "geometry":
		case "geometrycollection":
		case "linestring":
		case "multilinestring":
		case "multipoint":
		case "multipolygon":
		case "polygon":
		case "point":
			return GeometryColumnDef.create(name, type, pos);
		case "float":
		case "double":
			return FloatColumnDef.create(name, type, pos);
		case "decimal":
			return DecimalColumnDef.create(name, type, pos);
		case "date":
			return DateColumnDef.create(name, type, pos);
		case "datetime":
		case "timestamp":
			return DateTimeColumnDef.create(name, type, pos, columnLength);
		case "time":			
			return TimeColumnDef.create(name, type, pos, columnLength);
		case "year":
			return YearColumnDef.create(name, type, pos);
		case "enum":
			return EnumColumnDef.create(name, type, pos, enumValues);
		case "set":
			return SetColumnDef.create(name, type, pos, enumValues);
		case "bit":
			return BitColumnDef.create(name, type, pos);
		case "json":
			return JsonColumnDef.create(name, type, pos);

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

	public ColumnDef withName(String name) {
		return cloneSelfAndSet(clone -> {
			clone.name = name;
		});
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return dynamicEnum.get(type);
	}

	public int getPos() {
		return pos;
	}

	public ColumnDef withPos(short i) {
		if (pos == i) {
			return this;
		}
		return cloneSelfAndSet(clone -> {
			clone.pos = i;
		});
	}

	protected <T extends ColumnDef> T cloneSelfAndSet(Consumer<T> mutator) {
		T clone = (T) clone();
		mutator.accept(clone);
		return (T) getInterner().intern(clone);
	}
}
