package com.zendesk.maxwell.schema.columndef;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringColumnDef extends ColumnDef {
	public String charset;

	public StringColumnDef(String name, String type, short pos, String charset, boolean nullable) {
		super(name, type, pos, nullable);
		this.charset = charset;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDefaultCharset(String e) {
		if ( this.charset == null )
		  this.charset = e;
	}

	@Override
	public String toSQL(Object value) {
		byte[] b = (byte[]) value;

		if ( charset.equals("utf8") || charset.equals("utf8mb4")) {
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
			return Charset.forName("Windows-1252");
		case "ucs2":
			return Charset.forName("UTF-16");
		case "ujis":
			return Charset.forName("EUC-JP");
		default:
			try {
				return Charset.forName(charset.toLowerCase());
			} catch ( java.nio.charset.UnsupportedCharsetException e ) {
				throw new RuntimeException("error: unhandled character set '" + charset + "'");
			}
		}
	}
	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {

		if ( value instanceof String ) {
			return value;
		} else if ( value instanceof byte[] ) {
			byte[] b = (byte[]) value;
			if (charset.equals("binary")) {
				return Base64.encodeBase64String(b);
			} else {
				return new String(b, charsetForCharset());
			}
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}

	private String quoteString(String s) {
		String escaped = s.replaceAll("'", "''");
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}

}
