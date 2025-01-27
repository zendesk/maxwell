package com.zendesk.maxwell.schema.columndef;

import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class StringColumnDef extends ColumnDef {
	// mutability only allowed after clone and prior to insertion to interner
	private String charset;

	private StringColumnDef(String name, String type, short pos, String charset) {
		super(name, type, pos);
		this.charset = charset;
	}

	public static StringColumnDef create(String name, String type, short pos, String charset) {
		StringColumnDef temp = new StringColumnDef(name, type, pos, charset);
		return (StringColumnDef) INTERNER.intern(temp);
	}

	@Override
	public boolean equals(Object o) {
		if (o.getClass() == getClass()) {
			StringColumnDef other = (StringColumnDef) o;
			return super.equals(other)
					&& Objects.equals(charset, other.charset);
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		return 31 * hash + Objects.hash(charset);
	}

	public String getCharset() {
		return charset;
	}

	public StringColumnDef withCharset(String charset) {
		return cloneSelfAndSet(clone -> {
			clone.charset = charset;
		});
	}

	public StringColumnDef withDefaultCharset(String charset) {
		if ( this.charset == null ) {
			return cloneSelfAndSet(clone -> {
				clone.charset = charset;
			});
		} else {
			return this;
		}
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
		case "utf8": case "utf8mb3": case "utf8mb4":
			return StandardCharsets.UTF_8;
		case "latin1": case "ascii":
			return Charset.forName("Windows-1252");
		case "ucs2":
			return StandardCharsets.UTF_16;
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
