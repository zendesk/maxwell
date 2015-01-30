package com.zendesk.exodus.schema.columndef;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.common.util.MySQLConstants;

public class StringColumnDef extends ColumnDef {
	private final String encoding;

	public StringColumnDef(String tableName, String name, String type, int pos, String encoding) {
		super(tableName, name, type, pos);
		this.encoding = encoding;
	}

	public String getEncoding() {
		return encoding;
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BLOB ||
			   type == MySQLConstants.TYPE_VARCHAR ||
			   type == MySQLConstants.TYPE_STRING;
	}

	@Override
	public String toSQL(Object value) {
        byte[] b = (byte[]) value;

        if ( getEncoding().equals("utf8") || getEncoding().equals("utf8mb4")) {
        	return quoteString(new String(b));
        } else {
        	return "x'" +  Hex.encodeHexString( b ) + "'";
        }
	}

	private String quoteString(String s) {
		String escaped = StringEscapeUtils.escapeSql(s);
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}
}
