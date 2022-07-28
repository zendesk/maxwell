package com.zendesk.maxwell.replay;

import com.amazonaws.util.StringInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.io.StreamTokenizer.TT_EOF;
import static java.io.StreamTokenizer.TT_WORD;

/**
 * @author udyr@shlaji.com
 */
public class FilePathParser {
	private final String path;
	private StreamTokenizer tokenizer;

	private FilePathParser(String path) {
		this.path = path;
	}

	public static List<ReplayFilePattern> parse(String path) {
		return new FilePathParser(path).parse();
	}

	private List<ReplayFilePattern> parse() {
		try (InputStreamReader inputStream = new InputStreamReader(new StringInputStream(path))) {
			this.tokenizer = new StreamTokenizer(inputStream);
			return doParse(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private List<ReplayFilePattern> doParse(InputStreamReader inputStream) throws IOException {
		tokenizer.ordinaryChars('0', '9');
		tokenizer.wordChars('0', '9');

		tokenizer.wordChars('/', '/');
		tokenizer.wordChars('\\', '\\');
		tokenizer.wordChars(':', ':');
		tokenizer.whitespaceChars(',', ',');

		tokenizer.ordinaryChars('^', '^');
//		tokenizer.ordinaryChars('$', '$');

		tokenizer.quoteChar('`');
		tokenizer.quoteChar('\'');
		tokenizer.quoteChar('"');
		tokenizer.commentChar('#');

		List<ReplayFilePattern> patterns = new ArrayList<>();
		StringBuilder basePath = new StringBuilder();
		tokenizer.nextToken();
		while (true) {
			Pattern pattern = null;
			if (tokenizer.ttype == TT_EOF) {
				if (basePath.length() > 0) {
					patterns.add(new ReplayFilePattern(basePath.toString(), pattern));
				}
				break;
			}
			if (tokenizer.ttype == '^') {
				pattern = Pattern.compile(parseRegexp(inputStream));
				tokenizer.nextToken();
			}

			if (tokenizer.ttype != TT_WORD) {
				throw new IOException("Wrong file path " + (tokenizer.ttype > 0 ? String.valueOf(tokenizer.ttype) : tokenizer.sval));
			}

			if (basePath.length() > 0) {
				patterns.add(new ReplayFilePattern(basePath.toString(), pattern));
				basePath = new StringBuilder();
			}
			basePath.append(tokenizer.sval);
			tokenizer.nextToken();
		}
		return patterns;
	}

	private String parseRegexp(InputStreamReader inputStream) throws IOException {
		char ch;
		StringBuilder s = new StringBuilder("^");
		while (true) {
			int charInt = inputStream.read();
			if (charInt == -1) {
				break;
			}
			ch = (char) charInt;
			if (ch == '$' || ch == ',') {
				break;
			}
			s.append(ch);
		}
		s.append("$");
		return s.toString();
	}
}
