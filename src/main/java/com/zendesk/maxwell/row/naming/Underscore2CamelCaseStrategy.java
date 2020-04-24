package com.zendesk.maxwell.row.naming;

/**
 * 
 * @author frank chen Dec 24,2019 8:53:17 PM
 */
public class Underscore2CamelCaseStrategy implements INamingStrategy {

	@Override
	public String apply(String oldName) {
		StringBuilder builder = new StringBuilder(oldName.length());
		for (int i = 0, len = oldName.length(); i < len; i++) {
			char c = oldName.charAt(i);
			if (c == '_' && i + 1 < len) {
				char n = oldName.charAt(i + 1);
				//
				// turn the char after underscore to be upper case
				// but if the underscore is the first char in the string, don't do it
				//
				if (i > 0 && n >= 'a' && n <= 'z')
					n = (char) (n - 'a' + 'A');
				builder.append(n);
				i++;
			} else {
				if (c != '_')// ignore the underscore
					builder.append(c);
			}
		}
		return builder.toString();
	}
}
