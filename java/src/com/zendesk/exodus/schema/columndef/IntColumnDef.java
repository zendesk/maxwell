package com.zendesk.exodus.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;


public class IntColumnDef extends ColumnDef {
	private final int bits;
	private final boolean signed;

	public IntColumnDef(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
		this.bits = bitsFromType(type);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		switch(this.bits) {
		case 8:
			return type == MySQLConstants.TYPE_TINY;
		case 16:
			return type == MySQLConstants.TYPE_SHORT;
		case 24:
			return type == MySQLConstants.TYPE_INT24;
		case 32:
			return type == MySQLConstants.TYPE_LONG;
		default:
			return false;
		}
	}

	private final static int bitsFromType(String type) {
		switch(type) {
		case "tinyint":
			return 8;
		case "smallint":
			return 16;
		case "mediumint":
			return 24;
		case "int":
			return 32;
		default:
			return 0;
		}
	}


}
