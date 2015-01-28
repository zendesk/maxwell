package com.zendesk.exodus.schema.column;

import com.google.code.or.common.util.MySQLConstants;

public abstract class Column {
	private final String tableName;
	private final String name;
	private final String type;
	private final int pos;

	public Column(String tableName, String name, String type, int pos) {
		this.tableName = tableName;
		this.name = name;
		this.type = type;
		this.pos = pos;
	}

	public abstract boolean matchesMysqlType(int type);

	public static Column build(String tableName, String name, String encoding, String type, int pos, boolean signed) {
		switch(type) {
		case "tinyint":
		case "smallint":
		case "mediumint":
		case "int":
			return new IntColumn(tableName, name, type, pos, signed);
		case "bigint":
			return new BigIntColumn(tableName, name, type, pos, signed);
		case "tinytext":
		case "text":
		case "mediumtext":
		case "longtext":
		case "varchar":
		case "char":
			return new StringColumn(tableName, name, type, pos, encoding);
		case "tinyblob":
		case "blob":
		case "mediumblob":
		case "longblob":
			return new StringColumn(tableName, name, type, pos, "BINARYSOMETHING");
		case "float":
		case "double":
			return new FloatColumn(tableName, name, type, pos);
		case "decimal":
			return new DecimalColumn(tableName, name, type, pos);
		case "datetime":
		case "timestamp":
			return new DateTimeColumn(tableName, name, type, pos);
		case "date":
		case "time":
		case "bit":
		case "year":
		default:
			throw new IllegalArgumentException("unsupported column type " + type);
		}
	}
	/*
	private int castFromInformationSchema(String type) {
		switch( type ) {
		case "tinyint":
			return MySQLConstants.TYPE_TINY;
		case "smallint":
			return MySQLConstants.TYPE_SHORT;
		case "mediumint":
			return MySQLConstants.TYPE_INT24;
		case "int":
			return MySQLConstants.TYPE_LONG;
		case "bigint":
			return MySQLConstants.TYPE_LONGLONG;
		case "text":
		case "mediumtext":
			return MySQLConstants.TYPE_BLOB;
		case "varchar":
			return MySQLConstants.TYPE_VARCHAR;
		case "float":
			return MySQLConstants.TYPE_FLOAT;

		/*case
		 *
	  eq = case c[:data_type]
      when 'bigint'
        t[:type] == :longlong
      when 'int'
        t[:type] == :long
      when 'smallint'
        t[:type] == :short
      when 'mediumint'
        t[:type] == :int24
      when 'tinyint'
        t[:type] == :tiny
      when 'datetime'
        t[:type] == :datetime
      when 'text', 'mediumtext'
        t[:type] == :blob
      when 'varchar', 'float', 'timestamp'
        t[:type].to_s == c[:data_type]
      when 'date'
        t[:type] == :date
      when 'decimal'
        t[:type] == :decimal || t[:type] == :newdecimal
      else
        raise "unknown columns type '#{c[:data_type]}' (#{t[:type]})?"
      end


		}
	}		 */

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
}
