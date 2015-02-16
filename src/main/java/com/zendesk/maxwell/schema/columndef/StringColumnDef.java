package com.zendesk.maxwell.schema.columndef;

import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.common.util.MySQLConstants;

public class StringColumnDef extends ColumnDef {
	public StringColumnDef(String tableName, String name, String type, int pos, String encoding) {
		super(tableName, name, type, pos);
		this.encoding = encoding;
	}

	public void setDefaultEncoding(String e) {
		if ( this.encoding == null )
		  this.encoding = e;
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

	// this could obviously be more complete.
	private Charset charsetForEncoding() {
		switch(encoding) {
		case "utf8": case "utf8mb4":
			return Charset.forName("UTF-8");
		case "latin1":
			return Charset.forName("ISO-8859-1");
		default:
			return null;
		}
	}
	@Override
	public Object asJSON(Object value) {
		byte[] b = (byte[])value;

		return new String(b, charsetForEncoding());
	}

	private String quoteString(String s) {
		String escaped = StringEscapeUtils.escapeSql(s);
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}
}
