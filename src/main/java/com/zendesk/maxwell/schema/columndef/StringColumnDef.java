package com.zendesk.maxwell.schema.columndef;

import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.common.util.MySQLConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringColumnDef extends ColumnDef {
	static final Logger LOGGER = LoggerFactory.getLogger(StringColumnDef.class);
	public StringColumnDef(String tableName, String name, String type, int pos, String charset) {
		super(tableName, name, type, pos);
		this.charset = charset;
	}

	public void setDefaultCharset(String e) {
		if ( this.charset == null )
		  this.charset = e;
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

		if ( getCharset().equals("utf8") || getCharset().equals("utf8mb4")) {
			return quoteString(new String(b));
		} else {
			return "x'" +  Hex.encodeHexString( b ) + "'";
		}
	}

	// this could obviously be more complete.
	private Charset charsetForCharset() {
		switch(charset.toLowerCase()) {
		case "utf8": case "utf8mb4":
			return Charset.forName("UTF-8");
		case "latin1": case "ascii":
			return Charset.forName("ISO-8859-1");
		case "ucs2":
			return Charset.forName("UTF-16");
		default:
			LOGGER.warn("warning: unhandled character set '" + charset + "'");
			return null;
		}
	}
	@Override
	public Object asJSON(Object value) {

		if ( value instanceof String ) {
			return value;
		}

		byte[] b = (byte[])value;
		if ( charset.equals("binary") ) {
			return Base64.encodeBase64String(b);
		} else {
			return new String(b, charsetForCharset());
		}
	}

	private String quoteString(String s) {
		String escaped = StringEscapeUtils.escapeSql(s);
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}
}
