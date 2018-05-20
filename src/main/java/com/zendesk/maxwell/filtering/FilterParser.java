package com.zendesk.maxwell.filtering;

import com.amazonaws.util.StringInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.io.StreamTokenizer.*;

public class FilterParser {
	private final StreamTokenizer tokenizer;

	public FilterParser(
		String input
	) throws UnsupportedEncodingException {
		this.tokenizer = new StreamTokenizer(new InputStreamReader(new StringInputStream(input)));
	}

	public List<FilterPattern> parse() throws IOException {
		ArrayList<FilterPattern> patterns = new ArrayList<>();
		tokenizer.quoteChar('"');
		tokenizer.ordinaryChar('.');
		tokenizer.ordinaryChar('/');
		tokenizer.nextToken();

		FilterPattern p;
		while ( (p = parseFilterPattern()) != null )
			patterns.add(p);

		return patterns;
	}

	private void skipToken(char token) throws IOException {
		if ( tokenizer.ttype != token ) {
			throw new IOException("Expected '" + token + "', saw: " + tokenizer.toString());
		}
		tokenizer.nextToken();
	}


	private FilterPattern parseFilterPattern() throws IOException {
		FilterPatternType type;

		if ( tokenizer.ttype == TT_EOF )
			return null;

		if ( tokenizer.ttype != TT_WORD )
			throw new IOException();

		switch(tokenizer.sval.toLowerCase()) {
			case "include":
				type = FilterPatternType.INCLUDE;
				break;
			case "exclude":
				type = FilterPatternType.EXCLUDE;
				break;
			case "blacklist":
				type = FilterPatternType.BLACKLIST;
				break;
			default:
				throw new IOException("Unknown filter keyword: " + tokenizer.sval);
		}
		tokenizer.nextToken();


		skipToken(':');
		Pattern dbPattern = parsePattern();
		skipToken('.');
		Pattern tablePattern = parsePattern();

		if ( tokenizer.ttype == ',' )
			tokenizer.nextToken();

		return new FilterPattern(type, dbPattern, tablePattern);
	}

	private Pattern parsePattern() throws IOException {
		Pattern pattern;
		if ( tokenizer.ttype == '/' ) {
			pattern = Pattern.compile(parseRegexp());
		} else if ( tokenizer.ttype == '*' ) {
			pattern = Pattern.compile("");
		} else if ( tokenizer.ttype == TT_WORD ){
			pattern = Pattern.compile("^" + tokenizer.sval + "$");
		} else {
			throw new IOException();
		}
		tokenizer.nextToken();
		return pattern;
	}

	private String parseRegexp() throws IOException {
		String s = "";
		while ( true ) {
			tokenizer.nextToken();
			switch ( tokenizer.ttype ) {
				case TT_WORD:
					s += tokenizer.sval;
					break;
				case TT_NUMBER:
					s += tokenizer.nval;
					break;
				default:
					if (tokenizer.ttype == '/' || tokenizer.ttype == TT_EOL)
						return s;

					s += Character.toString((char) tokenizer.ttype);
					break;
			}
		}
	}
}
